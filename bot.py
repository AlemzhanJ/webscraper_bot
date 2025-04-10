import asyncio
import logging
import aiohttp # Use aiohttp for async requests
# Removed ssl import and patch
from bs4 import BeautifulSoup
import io
import re
from urllib.parse import urlparse, urljoin
from collections import deque # Use deque for queue
import datetime

from aiogram import Bot, Dispatcher, types
from aiogram.client.default import DefaultBotProperties
from aiogram.client.session.aiohttp import AiohttpSession # Import Aiogram's session wrapper
from aiogram.filters import CommandStart, Command
from aiogram.types import Message, BufferedInputFile, InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton
from aiogram.enums import ParseMode
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.state import State, StatesGroup

# Импортируем наши модули
from database import init_db, get_db, User, AISession, Message as DBMessage, get_cached_document, cache_document, get_cached_summary, cache_summary, cleanup_old_cache, CachedDocument, CachedSummary, cache_stats
from sqlalchemy import func
from ai_client import AIClient
from config import BOT_TOKEN, GEMINI_API_KEY, AI_MODEL, GEMINI_API_URL, AI_SYSTEM_PROMPT_TEMPLATE, MAX_SESSION_REQUESTS, SESSION_TIMEOUT_MINUTES, AI_SUMMARY_PROMPT, MAX_MESSAGE_LENGTH, ADMIN_IDS
from rate_limiter import rate_limiter

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Определяем состояния для FSM
class BotStates(StatesGroup):
    NORMAL = State()  # Обычное состояние
    AI_CHAT = State()  # Режим чата с ИИ

# Initialize dispatcher globally with FSM storage
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# Инициализация клиента ИИ
ai_client = AIClient(api_key=GEMINI_API_KEY, base_url=GEMINI_API_URL, model=AI_MODEL)

# Basic URL validation regex - Escaped hyphens inside character sets
URL_REGEX = r'^(https?://)?([\w\-]+\.)+[\w\-]+(/[\w\- ./?%&=]*)?$'

def is_valid_url(url):
    """Checks if the provided string is a valid URL."""
    # Basic check, can be improved
    return re.match(URL_REGEX, url) is not None

def get_text_from_html(html_content: str) -> str:
    """Extracts text content from HTML string with preserved structure."""
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Remove script, style, and nav elements
    for element in soup(["script", "style", "nav", "footer", "aside"]):
        element.decompose()
    
    # Добавим разделители для структурных элементов
    for header in soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6']):
        # Определяем уровень заголовка по тегу
        level = int(header.name[1])
        
        # Добавляем разделители в зависимости от уровня заголовка
        if level == 1:
            header.insert_before(soup.new_string('\n\n★' + '═' * 48 + '★\n'))
            header.insert_after(soup.new_string('\n★' + '═' * 48 + '★\n'))
        elif level == 2:
            header.insert_before(soup.new_string('\n\n┌' + '─' * 38 + '┐\n'))
            header.insert_after(soup.new_string('\n└' + '─' * 38 + '┘\n'))
        else:
            header.insert_before(soup.new_string('\n\n• • • • • •\n'))
            header.insert_after(soup.new_string('\n• • • • • •\n'))
    
    # Добавляем разделители для параграфов
    for paragraph in soup.find_all('p'):
        paragraph.insert_after(soup.new_string('\n\n'))
    
    # Добавляем разделители для списков
    for ul in soup.find_all('ul'):
        ul.insert_before(soup.new_string('\n┌─────────────────────────────┐\n'))
        ul.insert_after(soup.new_string('\n└─────────────────────────────┘\n'))
    
    for ol in soup.find_all('ol'):
        ol.insert_before(soup.new_string('\n┌─────────────────────────────┐\n'))
        ol.insert_after(soup.new_string('\n└─────────────────────────────┘\n'))
    
    for li in soup.find_all('li'):
        if li.parent.name == 'ol':
            # Для нумерованных списков используем цифры
            li.insert_before(soup.new_string('\n  ➜ '))
        else:
            # Для ненумерованных списков используем другой маркер
            li.insert_before(soup.new_string('\n  ◆ '))
    
    # Добавляем разделители для таблиц
    for table in soup.find_all('table'):
        table.insert_before(soup.new_string('\n\n┏' + '━' * 30 + ' ТАБЛИЦА ' + '━' * 30 + '┓\n'))
        table.insert_after(soup.new_string('\n┗' + '━' * 70 + '┛\n\n'))
    
    # Добавляем информацию о ссылках
    for a in soup.find_all('a', href=True):
        a.insert_after(soup.new_string(f" 🔗 [{a['href']}]"))
    
    # Обрабатываем изображения
    for img in soup.find_all('img'):
        alt_text = img.get('alt', 'Изображение')
        img.insert_before(soup.new_string(f'\n[🖼️ ИЗОБРАЖЕНИЕ: {alt_text}]\n'))
    
    # Обрабатываем блоки кода
    for code in soup.find_all('code'):
        code.insert_before(soup.new_string('\n```\n'))
        code.insert_after(soup.new_string('\n```\n'))
    
    for pre in soup.find_all('pre'):
        pre.insert_before(soup.new_string('\n```\n'))
        pre.insert_after(soup.new_string('\n```\n'))
    
    # Обрабатываем цитаты
    for blockquote in soup.find_all('blockquote'):
        blockquote.insert_before(soup.new_string('\n\n▌ '))
        lines = blockquote.get_text().split('\n')
        # Заменяем содержимое blockquote на форматированное
        blockquote.clear()
        for line in lines:
            blockquote.append(f'\n▌ {line}')
        blockquote.insert_after(soup.new_string('\n\n'))
    
    # Get text with preserved structure
    text = soup.get_text()
    
    # Break into lines and remove leading/trailing space on each
    lines = (line.strip() for line in text.splitlines())
    
    # Remove duplicated empty lines but keep meaningful structure
    processed_lines = []
    prev_line_empty = False
    
    for line in lines:
        is_empty = len(line) == 0
        is_separator = any(c in '─═━•┌┐└┘┏┓┗┛★◆➜' for c in line)
        
        # Всегда добавляем разделители
        if is_separator:
            processed_lines.append(line)
            prev_line_empty = False
        # Добавляем пустые строки только если предыдущая не была пустой
        elif is_empty:
            if not prev_line_empty:
                processed_lines.append(line)
                prev_line_empty = True
        # Добавляем непустые строки
        else:
            processed_lines.append(line)
            prev_line_empty = False
    
    # Объединяем с одинарными переносами строк
    text = '\n'.join(processed_lines)
    
    return text

def clean_text_for_telegram(text):
    """
    Очищает текст от символов, которые могут вызвать проблемы в Telegram.
    Удаляет * и # и другие маркдаун символы.
    """
    # Удаляем маркдаун символы
    text = re.sub(r'[*#]', '', text)
    
    # Удаление HTML тегов
    text = re.sub(r'<[^>]+>', '', text)
    
    # Удаление неподдерживаемых тегов Telegram
    text = re.sub(r'<(provider|script|style).*?>.*?</\1>', '', text, flags=re.DOTALL)
    
    # Замена обратных слешей и других специальных символов
    text = text.replace('\\', '\\\\').replace('`', '\\`')
    
    return text

def split_long_message(text, max_length=MAX_MESSAGE_LENGTH):
    """
    Разделяет длинное сообщение на части, чтобы соответствовать ограничениям Telegram.
    Возвращает список частей сообщения.
    """
    if len(text) <= max_length:
        return [text]
    
    parts = []
    
    # Разделение по абзацам, чтобы не разрывать предложения
    paragraphs = text.split('\n\n')
    current_part = ""
    
    for paragraph in paragraphs:
        # Если абзац сам по себе больше максимальной длины, разбиваем его по предложениям
        if len(paragraph) > max_length:
            sentences = re.split(r'([.!?])\s+', paragraph)
            i = 0
            while i < len(sentences):
                if i + 1 < len(sentences):
                    # Объединяем предложение с его знаком пунктуации
                    sentence = sentences[i] + sentences[i + 1]
                    i += 2
                else:
                    sentence = sentences[i]
                    i += 1
                
                if len(current_part + "\n\n" + sentence) > max_length:
                    parts.append(current_part)
                    current_part = sentence
                else:
                    if current_part:
                        current_part += "\n\n"
                    current_part += sentence
        else:
            # Добавляем абзац целиком, если он помещается
            if len(current_part + "\n\n" + paragraph) > max_length:
                parts.append(current_part)
                current_part = paragraph
            else:
                if current_part:
                    current_part += "\n\n"
                current_part += paragraph
    
    # Добавляем последнюю часть
    if current_part:
        parts.append(current_part)
    
    return parts

async def fetch_page(session: aiohttp.ClientSession, url: str) -> tuple[str | None, str | None]:
    """Fetches a single page asynchronously."""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
    }
    try:
        async with session.get(url, headers=headers, timeout=10, ssl=False) as response: # Added ssl=False for potential issues
            response.raise_for_status()
            content_type = response.headers.get('content-type', '').lower()
            if 'text/html' in content_type:
                return await response.text(), None
            else:
                return None, f"Content type is not HTML ({content_type})"
    except aiohttp.ClientError as e:
        logger.warning(f"HTTP Error fetching {url}: {e}")
        return None, f"HTTP Error: {e}"
    except asyncio.TimeoutError:
        logger.warning(f"Timeout fetching {url}")
        return None, "Timeout"
    except Exception as e:
        logger.error(f"Unexpected error fetching {url}: {e}")
        return None, f"Unexpected error: {e}"


async def crawl_website(start_url: str, max_pages: int = 1000, status_message: Message = None) -> tuple[str, int]:
    """Crawls a website starting from start_url, collecting text."""
    logger.info(f"Starting crawl for: {start_url} (max_pages={max_pages})")
    if not start_url.startswith(('http://', 'https://')):
        start_url = 'https://' + start_url

    parsed_start_url = urlparse(start_url)
    base_domain = parsed_start_url.netloc
    if not base_domain:
        return "Error: Invalid starting URL.", 0

    to_visit = deque([start_url])
    visited = set()
    scraped_data = {}
    pages_processed = 0
    errors = []
    
    # Переменные для отслеживания динамики обнаружения новых URL
    total_links_found = 0
    new_links_by_page = {}
    consecutive_low_discovery_pages = 0
    discovery_threshold = 3  # Порог для определения страниц с низким количеством новых ссылок
    max_consecutive_low_pages = 5  # Максимальное количество подряд идущих страниц с низким обнаружением
    
    # Для отслеживания прогресса
    last_update_time = datetime.datetime.now()
    update_interval = datetime.timedelta(seconds=2)  # Обновляем сообщение каждые 2 секунды
    
    # Оценка общего количества страниц (начинаем с количества в очереди)
    estimated_total_pages = len(to_visit)

    async with aiohttp.ClientSession() as session:
        while to_visit and pages_processed < max_pages:
            current_url = to_visit.popleft()

            if current_url in visited:
                continue

            # Normalize URL slightly (e.g., remove fragment)
            current_url = urljoin(current_url, urlparse(current_url).path)
            if not current_url.startswith(('http://', 'https://')): # Ensure scheme is present
                 continue

            if current_url in visited:
                continue

            # Обновляем статус прогресса
            current_time = datetime.datetime.now()
            if status_message and (current_time - last_update_time > update_interval or pages_processed == 0):
                last_update_time = current_time
                
                # Если обнаружено значительное количество страниц, обновляем оценку
                if len(to_visit) > estimated_total_pages - pages_processed:
                    estimated_total_pages = pages_processed + len(to_visit)
                
                # Рассчитываем процент на основе текущей оценки общего количества
                if estimated_total_pages > 0:
                    percent = min(int((pages_processed / estimated_total_pages) * 100), 99)
                else:
                    percent = 0
                
                # Создаем прогресс-бар
                progress_bar_length = 20
                filled = int(progress_bar_length * percent / 100)
                bar = '█' * filled + '░' * (progress_bar_length - filled)
                
                # Строим сообщение с обновлением статуса
                status_text = (
                    f"🔄 <b>Обработка сайта {base_domain}...</b>\n\n"
                    f"📊 Прогресс: {percent}% [{bar}]\n"
                    f"📑 Обработано страниц: <b>{pages_processed}</b>\n"
                    f"📈 Оценка общего кол-ва страниц: <b>~{estimated_total_pages}</b>\n"
                    f"🔗 Текущая страница: <code>{current_url}</code>\n\n"
                    f"⏳ Пожалуйста, подождите...\n"
                    f"<i>Бот автоматически определяет количество страниц</i>"
                )
                
                try:
                    await status_message.edit_text(status_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
                except Exception as e:
                    logger.warning(f"Couldn't update status message: {e}")

            logger.info(f"Processing ({pages_processed+1}/{estimated_total_pages}): {current_url}")
            visited.add(current_url)
            pages_processed += 1
            
            # Запоминаем сколько ссылок было до обработки этой страницы
            links_before = len(to_visit)

            html_content, error = await fetch_page(session, current_url)

            if error:
                errors.append(f"{current_url}: {error}")
                continue

            if html_content:
                # Получаем заголовок страницы
                try:
                    page_soup = BeautifulSoup(html_content, 'html.parser')
                    page_title = page_soup.title.string if page_soup.title else "Без заголовка"
                except:
                    page_title = "Без заголовка"
                
                # Добавляем разделитель страницы и URL перед текстом
                page_separator = "\n\n" + "╔" + "═" * 78 + "╗\n"
                page_header = f"{page_separator}║  СТРАНИЦА: {page_title.strip()}\n"
                page_header += f"║  URL: {current_url}\n"
                page_header += "╚" + "═" * 78 + "╝\n\n"
                
                # Получаем текст страницы и добавляем заголовок
                text = page_header + get_text_from_html(html_content)
                scraped_data[current_url] = text

                # Find links
                soup = BeautifulSoup(html_content, 'html.parser')
                for link in soup.find_all('a', href=True):
                    href = link['href']
                    absolute_url = urljoin(current_url, href)
                    parsed_absolute_url = urlparse(absolute_url)

                    # Check if internal link, http/https, and not visited/queued
                    if (parsed_absolute_url.netloc == base_domain and
                        parsed_absolute_url.scheme in ['http', 'https'] and
                        absolute_url not in visited and
                        absolute_url not in to_visit):
                        to_visit.append(absolute_url)
                
                # Подсчитываем, сколько новых ссылок найдено на этой странице
                links_after = len(to_visit)
                new_links = links_after - links_before
                new_links_by_page[current_url] = new_links
                total_links_found += new_links
                
                # Проверяем, является ли эта страница "малопродуктивной" в плане новых ссылок
                avg_links_per_page = total_links_found / max(1, pages_processed)
                if new_links < avg_links_per_page * 0.3 or new_links < discovery_threshold:
                    consecutive_low_discovery_pages += 1
                else:
                    consecutive_low_discovery_pages = 0
                
                # Если несколько страниц подряд дают мало новых ссылок и мы обработали достаточно страниц,
                # можно предположить, что мы исследовали большую часть сайта
                if consecutive_low_discovery_pages >= max_consecutive_low_pages and pages_processed > 50:
                    logger.info(f"Stopping crawl after {pages_processed} pages due to low discovery rate")
                    break
                    
            # Small delay to be polite to the server
            await asyncio.sleep(0.1)

    # Показываем финальный статус 100%
    if status_message:
        try:
            final_status = (
                f"✅ <b>Обработка сайта {base_domain} завершена!</b>\n\n"
                f"📊 Прогресс: 100% [{'█' * 20}]\n"
                f"📑 Обработано страниц: <b>{pages_processed}</b>\n"
                f"⏱ Завершено!\n\n"
                f"🔄 Подготовка результатов..."
            )
            await status_message.edit_text(final_status, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
        except Exception as e:
            logger.warning(f"Couldn't update final status message: {e}")

    # Собираем все скрапленные тексты в один документ с оглавлением
    all_text = ""
    
    # Создаем оглавление
    toc = "\n\n" + "╔" + "═" * 78 + "╗\n"
    toc += "║" + " " * 30 + "ОГЛАВЛЕНИЕ" + " " * 30 + "║\n"
    toc += "╚" + "═" * 78 + "╝\n\n"
    
    page_number = 1
    titles = {}
    for url, text in scraped_data.items():
        # Извлекаем заголовок страницы из текста
        try:
            title_line = text.split("\n")[3]  # Строка с "СТРАНИЦА: ..."
            title = title_line.replace("СТРАНИЦА: ", "").strip()
        except:
            title = f"Страница {page_number}"
            
        # Сохраняем для оглавления
        titles[url] = (title, page_number)
        page_number += 1
    
    # Сортируем по номеру страницы
    for url, (title, page_num) in sorted(titles.items(), key=lambda x: x[1][1]):
        toc += f"  {page_num:02d}. {title}\n"
    
    toc += "\n" + "╔" + "═" * 78 + "╗\n"
    toc += "║" + " " * 25 + "ИНФОРМАЦИЯ О ДОКУМЕНТЕ" + " " * 25 + "║\n"
    toc += "╚" + "═" * 78 + "╝\n\n"
    
    # Добавляем информацию о сайте
    domain_info = f"📋 Сайт: {base_domain}\n"
    domain_info += f"📊 Обработано страниц: {pages_processed}\n\n"
    
    # Собираем итоговый документ
    all_text = domain_info + toc + "\n\n" + "╔" + "═" * 78 + "╗\n"
    all_text += "║" + " " * 27 + "СОДЕРЖИМОЕ САЙТА" + " " * 27 + "║\n"
    all_text += "╚" + "═" * 78 + "╝\n\n"
    
    all_text += "\n".join(scraped_data.values())
    
    # Если были ошибки, добавляем их в конец
    if errors:
        error_section = "\n\n" + "╔" + "═" * 78 + "╗\n"
        error_section += "║" + " " * 26 + "ОШИБКИ ПРИ СКАНИРОВАНИИ" + " " * 25 + "║\n"
        error_section += "╚" + "═" * 78 + "╝\n\n"
        for error in errors:
            error_section += f"⚠️ {error}\n"
        all_text += error_section
    
    logger.info(f"Crawl completed: {pages_processed} pages processed")
    return all_text, pages_processed

def get_completion_reason(pages_processed, max_pages, consecutive_low_discovery):
    """Возвращает причину завершения сканирования."""
    if pages_processed >= max_pages:
        return "Достигнут максимальный лимит страниц"
    elif consecutive_low_discovery >= 5:
        return "Исчерпаны доступные страницы сайта"
    else:
        return "Завершено успешно"

# Функция для работы с сессиями ИИ
async def create_ai_session(user_id, username, first_name, document_text):
    """Создает новую сессию чата с ИИ."""
    db = get_db()
    
    # Проверяем существование пользователя или создаем его
    user = db.query(User).filter(User.telegram_id == user_id).first()
    if not user:
        user = User(telegram_id=user_id, username=username, first_name=first_name)
        db.add(user)
        db.commit()
        db.refresh(user)
    
    # Закрываем все активные сессии пользователя
    active_sessions = db.query(AISession).filter(
        AISession.user_id == user.id,
        AISession.is_active == True
    ).all()
    
    # Удаляем сообщения из активных сессий и закрываем их
    for session in active_sessions:
        # Удаляем сообщения
        db.query(DBMessage).filter(
            DBMessage.session_id == session.id
        ).delete()
        
        # Закрываем сессию
        session.is_active = False
    
    # Удаляем старые сессии (оставляем последние 5)
    old_sessions = db.query(AISession).filter(
        AISession.user_id == user.id,
        AISession.is_active == False
    ).order_by(AISession.last_activity.desc()).offset(5).all()
    
    for old_session in old_sessions:
        # Удаляем сообщения
        db.query(DBMessage).filter(
            DBMessage.session_id == old_session.id
        ).delete()
        
        # Удаляем сессию
        db.delete(old_session)
    
    # Используем datetime без UTC для совместимости с БД
    current_time = datetime.datetime.now()
    
    # Создаем новую сессию
    new_session = AISession(
        user_id=user.id,
        document_text=document_text,
        created_at=current_time,
        last_activity=current_time,
        is_active=True,
        request_count=0
    )
    
    db.add(new_session)
    db.commit()
    db.refresh(new_session)
    
    logger.info(f"Created new AI session {new_session.id} for user {user_id}")
    return new_session.id

async def get_active_session(user_id):
    """Получает активную сессию для пользователя."""
    db = get_db()
    user = db.query(User).filter(User.telegram_id == user_id).first()
    
    if not user:
        return None
    
    # Проверяем таймаут сессии - используем datetime без UTC для совместимости с БД
    timeout = datetime.datetime.now() - datetime.timedelta(minutes=SESSION_TIMEOUT_MINUTES)
    
    # Находим активную сессию
    session = db.query(AISession).filter(
        AISession.user_id == user.id,
        AISession.is_active == True
    ).first()
    
    # Если сессия не найдена
    if not session:
        return None
    
    # Проверяем таймаут и лимит запросов
    if session.last_activity <= timeout or session.request_count >= MAX_SESSION_REQUESTS:
        # Удаляем сообщения
        deleted_count = db.query(DBMessage).filter(
            DBMessage.session_id == session.id
        ).delete()
        
        # Закрываем сессию
        session.is_active = False
        db.commit()
        
        reason = "timeout" if session.last_activity <= timeout else "request limit exceeded"
        logger.info(f"Session {session.id} closed due to {reason}. Deleted {deleted_count} messages.")
        return None
    
    return session

async def add_message_to_session(session_id, role, content):
    """Добавляет сообщение в сессию."""
    db = get_db()
    session = db.query(AISession).filter(AISession.id == session_id).first()
    
    if not session:
        return False
    
    # Обновляем время последней активности
    session.last_activity = datetime.datetime.now()
    
    # Если это сообщение от пользователя, увеличиваем счетчик запросов
    if role == 'user':
        session.request_count += 1
    
    # Добавляем сообщение
    message = DBMessage(
        session_id=session_id,
        role=role,
        content=content,
        timestamp=datetime.datetime.now()
    )
    
    db.add(message)
    db.commit()
    
    return True

async def get_session_messages(session_id):
    """Получает все сообщения из сессии."""
    db = get_db()
    messages = db.query(DBMessage).filter(
        DBMessage.session_id == session_id
    ).order_by(DBMessage.timestamp).all()
    
    return [{'role': msg.role, 'content': msg.content} for msg in messages]

async def close_session(user_id):
    """Закрывает активную сессию пользователя и удаляет связанные сообщения."""
    db = get_db()
    user = db.query(User).filter(User.telegram_id == user_id).first()
    
    if not user:
        return False
    
    session = db.query(AISession).filter(
        AISession.user_id == user.id,
        AISession.is_active == True
    ).first()
    
    if session:
        # Удаляем все сообщения, связанные с этой сессией
        deleted_messages = db.query(DBMessage).filter(
            DBMessage.session_id == session.id
        ).delete()
        
        # Помечаем сессию как неактивную
        session.is_active = False
        db.commit()
        
        logger.info(f"Session {session.id} for user {user_id} closed. Deleted {deleted_messages} messages.")
        return True
    
    return False

async def send_long_message(message, text, reply_markup=None):
    """
    Отправляет длинное сообщение, разбивая его на части при необходимости.
    """
    # Очищаем текст от проблемных символов
    clean_text = clean_text_for_telegram(text)
    
    # Разбиваем на части, если необходимо
    parts = split_long_message(clean_text)
    
    # Отправляем каждую часть
    for i, part in enumerate(parts):
        # Добавляем маркер "продолжение", если это не первая часть
        if i > 0:
            part = "... " + part
        
        # Добавляем маркер "продолжение следует", если это не последняя часть
        if i < len(parts) - 1:
            part += " ..."
        
        # Для последней части добавляем reply_markup, если он есть
        if i == len(parts) - 1 and reply_markup:
            await message.answer(part, reply_markup=reply_markup, disable_web_page_preview=True)
        else:
            await message.answer(part, disable_web_page_preview=True)

@dp.message(CommandStart())
async def send_welcome(message: Message, state: FSMContext):
    """Обработчик команды /start."""
    user_id = message.from_user.id
    
    # Проверяем лимиты для защиты от DoS/DDoS
    is_allowed, reason, ban_time = rate_limiter.add_request(user_id, "general_requests")
    if not is_allowed:
        if reason == "banned":
            await message.answer(f"⚠️ Вы временно заблокированы из-за превышения лимита запросов. Повторите через {ban_time} секунд.")
        return
    
    logger.info(f"User {user_id} (@{message.from_user.username}) started the bot")
    
    await state.set_state(BotStates.NORMAL)
    
    welcome_message = (
        f"👋 <b>Добро пожаловать, {message.from_user.first_name}!</b>\n\n"
        f"🔍 Я - бот для <b>скрейпинга веб-сайтов</b> с функциями ИИ.\n\n"
        f"<b>Мои возможности:</b>\n"
        f"📚 Сбор текстового контента с сайтов\n"
        f"🤖 Ответы на вопросы по собранным данным\n"
        f"📝 Создание конспектов документов\n\n"
        f"<b>Как использовать:</b>\n"
        f"1️⃣ Отправьте мне URL сайта\n"
        f"2️⃣ Дождитесь завершения сбора данных\n"
        f"3️⃣ Используйте кнопки для анализа контента\n\n"
        f"<i>Начните сейчас - просто отправьте URL!</i>"
    )
    
    await message.answer(welcome_message, parse_mode=ParseMode.HTML)

@dp.message(Command("stop"))
async def handle_stop_command(message: Message, state: FSMContext):
    """Обработчик команды /stop - закрывает сессию ИИ."""
    current_state = await state.get_state()
    
    if current_state == BotStates.AI_CHAT.state:
        await close_session(message.from_user.id)
        await state.set_state(BotStates.NORMAL)
        
        # Удаляем клавиатуру
        await message.answer(
            "✅ <b>Сессия с ИИ завершена.</b>\n"
            "Теперь вы можете отправить мне URL для сбора информации.",
            reply_markup=types.ReplyKeyboardRemove(),
            parse_mode=ParseMode.HTML
        )
    else:
        await message.answer(
            "❓ У вас нет активной сессии с ИИ.\n"
            "Отправьте URL сайта, чтобы начать работу."
        )

@dp.message(Command("admin"))
async def admin_command(message: Message):
    """Команда для просмотра статистики администратором."""
    user_id = message.from_user.id
    
    # Проверяем, является ли пользователь администратором
    if user_id not in ADMIN_IDS:
        logger.warning(f"Unauthorized admin access attempt from user {user_id} (@{message.from_user.username})")
        return
    
    # Получаем статистику от rate_limiter
    stats = rate_limiter.get_stats()
    
    # Получаем статистику из базы данных
    db = get_db()
    users_count = db.query(User).count()
    active_sessions = db.query(AISession).filter(AISession.is_active == True).count()
    total_sessions = db.query(AISession).count()
    total_messages = db.query(DBMessage).count()
    
    # Формируем ответ
    admin_message = (
        f"📊 <b>Статистика бота:</b>\n\n"
        f"👤 <b>Пользователи:</b>\n"
        f"- Всего пользователей в БД: <code>{users_count}</code>\n"
        f"- Активных пользователей: <code>{stats['total_users']}</code>\n"
        f"- Заблокированных пользователей: <code>{stats['banned_users']}</code>\n\n"
        
        f"💬 <b>Сессии и сообщения:</b>\n"
        f"- Активных сессий ИИ: <code>{active_sessions}</code>\n"
        f"- Всего сессий: <code>{total_sessions}</code>\n"
        f"- Всего сообщений: <code>{total_messages}</code>\n\n"
        
        f"🔄 <b>Запросы:</b>\n"
        f"- URL запросы: <code>{stats['requests_by_type']['url_requests']}</code>\n"
        f"- ИИ запросы: <code>{stats['requests_by_type']['ai_requests']}</code>\n"
        f"- Общие запросы: <code>{stats['requests_by_type']['general_requests']}</code>\n\n"
        
        f"⏱ <b>Данные собраны:</b> <code>{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</code>"
    )
    
    await message.answer(admin_message, parse_mode=ParseMode.HTML)

@dp.message(lambda message: message.text == "🛑 Завершить сессию")
async def handle_end_session_button(message: Message, state: FSMContext):
    """Обработчик нажатия на кнопку завершения сессии."""
    current_state = await state.get_state()
    
    if current_state == BotStates.AI_CHAT.state:
        await close_session(message.from_user.id)
        await state.set_state(BotStates.NORMAL)
        
        # Удаляем клавиатуру
        await message.answer(
            "✅ <b>Сессия с ИИ завершена.</b>\n"
            "Теперь вы можете отправить мне URL для сбора информации.",
            reply_markup=types.ReplyKeyboardRemove(),
            parse_mode=ParseMode.HTML
        )

@dp.callback_query(lambda c: c.data == 'ask_ai')
async def process_ask_ai_button(callback_query: types.CallbackQuery, state: FSMContext):
    """Обработчик нажатия на инлайн кнопку 'Спросить ИИ'."""
    user_id = callback_query.from_user.id
    
    # Проверяем лимиты для защиты от DoS/DDoS
    is_allowed, reason, ban_time = rate_limiter.add_request(user_id, "general_requests")
    if not is_allowed:
        if reason == "banned":
            await callback_query.message.answer(f"⚠️ Вы временно заблокированы из-за превышения лимита запросов. Повторите через {ban_time} секунд.")
        await callback_query.answer()
        return
    
    # Сразу отвечаем на callback query, чтобы избежать таймаута
    await callback_query.answer()
    
    # Получаем текст документа из состояния
    data = await state.get_data()
    document_text = data.get("document_text")
    
    if not document_text:
        await callback_query.message.answer("❌ Сначала отправьте URL для сбора информации.")
        return
    
    # Создаем сессию в БД
    session_id = await create_ai_session(
        callback_query.from_user.id,
        callback_query.from_user.username,
        callback_query.from_user.first_name,
        document_text
    )
    
    # Сохраняем ID сессии в состоянии
    await state.update_data(ai_session_id=session_id)
    
    # Переводим бота в режим AI_CHAT
    await state.set_state(BotStates.AI_CHAT)
    
    # Создаем клавиатуру с кнопкой завершения сессии
    keyboard = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="🛑 Завершить сессию")]],
        resize_keyboard=True
    )
    
    ai_chat_message = (
        f"🤖 <b>Режим чата с ИИ активирован!</b>\n\n"
        f"📝 Задайте вопрос по содержимому документа.\n\n"
        f"ℹ️ <b>Информация о сессии:</b>\n"
        f"📊 Доступно запросов: <b>{MAX_SESSION_REQUESTS}</b>\n"
        f"⏱ Время бездействия до закрытия: <b>{SESSION_TIMEOUT_MINUTES} мин</b>\n\n"
        f"<i>Для выхода нажмите кнопку ниже.</i>"
    )
    
    await callback_query.message.answer(
        ai_chat_message,
        reply_markup=keyboard,
        parse_mode=ParseMode.HTML
    )

@dp.callback_query(lambda c: c.data == 'get_summary')
async def process_summary_button(callback_query: types.CallbackQuery, state: FSMContext):
    """Обработчик нажатия на инлайн кнопку 'Конспект'."""
    user_id = callback_query.from_user.id
    
    # Проверяем лимиты для защиты от DoS/DDoS
    is_allowed, reason, ban_time = rate_limiter.add_request(user_id, "ai_requests")
    if not is_allowed:
        if reason == "banned":
            await callback_query.message.answer(f"⚠️ Вы временно заблокированы из-за превышения лимита запросов. Повторите через {ban_time} секунд.")
        await callback_query.answer()
        return
    
    # Сразу отвечаем на callback query, чтобы избежать таймаута
    await callback_query.answer()
    
    # Получаем текст документа, ID кэшированного документа и тип обхода из состояния
    data = await state.get_data()
    document_text = data.get("document_text")
    cached_document_id = data.get("cached_document_id")
    
    # Проверяем, является ли это обходом одной страницы
    is_single_page = data.get("is_single_page", False)
    
    if not document_text:
        await callback_query.message.answer("Сначала отправьте URL для сбора информации.")
        return
    
    # Создаем и отправляем статусное сообщение с индикатором загрузки
    loading_message = await callback_query.message.answer(
        "🤖 Генерирую конспект документа...\n\n"
        "⏳ Это может занять около минуты. Пожалуйста, подождите..."
    )
    
    # Если есть ID кэшированного документа, проверяем кэш конспектов
    if cached_document_id:
        cached_summary = get_cached_summary(cached_document_id, is_single_page=is_single_page)
        if cached_summary:
            logger.info(f"Using cached summary for document {cached_document_id} (single_page: {is_single_page})")
            
            # Обновляем сообщение о загрузке
            await loading_message.edit_text(
                "✅ Конспект готов! Отправляю результаты...\n"
                "<i>Данные получены из кэша</i>", 
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True
            )
            
            # Отправляем ответ пользователю
            await send_long_message(callback_query.message, cached_summary)
            
            # Удаляем сообщение о загрузке
            await callback_query.bot.delete_message(chat_id=callback_query.message.chat.id, message_id=loading_message.message_id)
            
            return
    
    # Если конспекта нет в кэше или нет ID кэшированного документа, генерируем новый
    
    # Отправляем индикатор набора текста
    await callback_query.bot.send_chat_action(chat_id=callback_query.message.chat.id, action="typing")
    
    # Формируем системный промпт с текстом документа для конспекта
    system_prompt = AI_SUMMARY_PROMPT.format(document_text=document_text)
    
    # Запрашиваем ответ от ИИ (конспект)
    try:
        ai_response = await ai_client.get_completion([{"role": "user", "content": "Составь конспект"}], system_prompt)
        
        # Проверяем, что ответ не содержит сообщение об ошибке
        if ai_response and not ai_response.startswith("Ошибка при получении ответа"):
            # Если есть ID кэшированного документа, сохраняем конспект в кэш
            if cached_document_id:
                cache_summary(cached_document_id, ai_response, is_single_page=is_single_page)
                logger.info(f"Cached summary for document {cached_document_id} (single_page: {is_single_page})")
            
            # Обновляем сообщение о загрузке
            await loading_message.edit_text("✅ Конспект готов! Отправляю результаты...", disable_web_page_preview=True)
            
            # Отправляем ответ пользователю
            await send_long_message(callback_query.message, ai_response)
            
            # Удаляем сообщение о загрузке
            await callback_query.bot.delete_message(chat_id=callback_query.message.chat.id, message_id=loading_message.message_id)
        else:
            # Если в ответе содержится ошибка, показываем её
            error_message = ai_response if ai_response else "Неизвестная ошибка при создании конспекта"
            await loading_message.edit_text(f"❌ {error_message}", disable_web_page_preview=True)
            logger.error(f"AI response error for user {user_id}: {ai_response}")
    except Exception as e:
        logger.exception(f"Error generating summary for user {user_id}: {e}")
        await loading_message.edit_text(f"❌ Произошла ошибка при создании конспекта: {e}", disable_web_page_preview=True)

@dp.message(BotStates.AI_CHAT)
async def handle_ai_chat_message(message: Message, state: FSMContext):
    """Обрабатывает сообщения пользователя в режиме чата с ИИ."""
    user_id = message.from_user.id
    
    # Проверяем лимиты для защиты от DoS/DDoS
    is_allowed, reason, ban_time = rate_limiter.add_request(user_id, "ai_requests")
    if not is_allowed:
        if reason == "banned":
            await message.answer(f"⚠️ Вы временно заблокированы из-за превышения лимита запросов. Повторите через {ban_time} секунд.")
        return
    
    # Получаем данные из состояния
    data = await state.get_data()
    session_id = data.get("ai_session_id")
    
    if not session_id:
        await message.answer("Ошибка: сессия не найдена. Пожалуйста, начните заново.")
        await state.set_state(BotStates.NORMAL)
        return
    
    # Проверяем, что сессия активна
    session = await get_active_session(user_id)
    if not session:
        await message.answer(
            "Ваша сессия с ИИ была завершена из-за таймаута или превышения лимита запросов. "
            "Начните новую сессию, отправив URL.",
            reply_markup=types.ReplyKeyboardRemove()
        )
        await state.set_state(BotStates.NORMAL)
        return
    
    # Отправляем сообщение о том, что запрос обрабатывается
    loading_message = await message.answer(
        f"🤔 Обдумываю ваш вопрос... ({session.request_count + 1}/{MAX_SESSION_REQUESTS})"
    )
    
    # Добавляем сообщение пользователя в БД
    await add_message_to_session(session_id, 'user', message.text)
    
    # Получаем все сообщения из сессии
    messages = await get_session_messages(session_id)
    
    # Формируем системный промпт с текстом документа
    system_prompt = AI_SYSTEM_PROMPT_TEMPLATE.format(document_text=session.document_text)
    
    # Отправляем индикатор набора текста
    await message.bot.send_chat_action(chat_id=message.chat.id, action="typing")
    
    # Запрашиваем ответ от ИИ
    ai_response = await ai_client.get_completion(messages, system_prompt)
    
    # Добавляем ответ ИИ в БД
    await add_message_to_session(session_id, 'assistant', ai_response)
    
    # Удаляем сообщение о загрузке
    await message.bot.delete_message(chat_id=message.chat.id, message_id=loading_message.message_id)
    
    # Отправляем ответ пользователю с обработкой длинных сообщений
    await send_long_message(message, ai_response)
    
    # Проверяем, не исчерпан ли лимит запросов
    if session.request_count >= MAX_SESSION_REQUESTS:
        await message.answer(
            f"⚠️ Вы использовали все {MAX_SESSION_REQUESTS} запросов в этой сессии. Сессия завершена.",
            reply_markup=types.ReplyKeyboardRemove()
        )
        await close_session(user_id)
        await state.set_state(BotStates.NORMAL)

@dp.message()
async def handle_message(message: Message, state: FSMContext):
    """Обрабатывает входящие сообщения, ожидая URL."""
    user_id = message.from_user.id
    
    # Пропускаем команды
    if message.text and message.text.startswith('/'):
        return
    
    # Проверяем текущее состояние
    current_state = await state.get_state()
    if current_state == BotStates.AI_CHAT.state:
        # Делегируем обработку функции для чата с ИИ
        await handle_ai_chat_message(message, state)
        return
    
    # Проверяем лимиты для защиты от DoS/DDoS
    is_allowed, reason, ban_time = rate_limiter.add_request(user_id, "general_requests")
    if not is_allowed:
        if reason == "banned":
            await message.answer(f"⚠️ Вы временно заблокированы из-за превышения лимита запросов. Повторите через {ban_time} секунд.")
        return
    
    url = message.text
    if not url or not is_valid_url(url):
        await message.answer("Пожалуйста, отправьте корректный URL (например, https://example.com).")
        return

    # Add scheme if missing for initial validation and filename generation
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url

    parsed_initial_url = urlparse(url)
    if not all([parsed_initial_url.scheme, parsed_initial_url.netloc]):
        await message.answer("Некорректный URL. Убедитесь, что он содержит http:// или https:// и доменное имя.")
        return

    # Проверяем лимиты для URL запросов
    is_allowed, reason, ban_time = rate_limiter.add_request(user_id, "url_requests")
    if not is_allowed:
        if reason == "banned":
            await message.answer(f"⚠️ Вы временно заблокированы из-за превышения лимита запросов. Повторите через {ban_time} секунд.")
        elif reason.startswith("rate_limit_exceeded"):
            await message.answer(f"⚠️ Превышен лимит запросов. Пожалуйста, подождите некоторое время перед повторной попыткой.")
        return

    # Сохраняем URL в состоянии
    await state.update_data(url=url)
    
    # Создаем инлайн кнопки для выбора режима скрейпинга
    inline_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📄 Только текущая страница", callback_data="crawl_single")],
        [InlineKeyboardButton(text="🌐 Полный обход сайта", callback_data="crawl_full")]
    ])
    
    # Отправляем сообщение с выбором режима
    choice_message = (
        f"🔍 <b>Выберите режим обработки для сайта:</b> <code>{parsed_initial_url.netloc}</code>\n\n"
        f"<b>📄 Только текущая страница</b>\n"
        f"- Быстрое получение контента только с указанной страницы\n"
        f"- Подходит для обработки отдельных статей или документов\n\n"
        f"<b>🌐 Полный обход сайта</b>\n"
        f"- Автоматическое определение и обход всех доступных страниц\n"
        f"- Подходит для анализа всего сайта (займет больше времени)\n\n"
        f"👇 <b>Сделайте выбор:</b>"
    )
    
    await message.answer(choice_message, reply_markup=inline_kb, parse_mode=ParseMode.HTML, disable_web_page_preview=True)


@dp.callback_query(lambda c: c.data == 'crawl_single')
async def process_single_page_crawl(callback_query: types.CallbackQuery, state: FSMContext):
    """Обработчик выбора обхода только одной страницы."""
    user_id = callback_query.from_user.id
    
    # Сразу отвечаем на callback query, чтобы избежать таймаута
    await callback_query.answer()
    
    # Получаем URL из состояния
    data = await state.get_data()
    url = data.get("url")
    
    if not url:
        await callback_query.message.answer("Ошибка: URL не найден. Пожалуйста, отправьте URL заново.")
        return

    parsed_url = urlparse(url)
    
    # Проверяем кэш для этого URL (с флагом is_single_page=True)
    cached_doc = get_cached_document(url, is_single_page=True)
    
    # Если документ найден в кэше
    if cached_doc:
        logger.info(f"Using cached document for URL {url} (single page)")
        
        # Сохраняем текст документа в состоянии с флагом одиночной страницы
        await state.update_data(
            document_text=cached_doc["content"], 
            cached_document_id=cached_doc["id"],
            is_single_page=True
        )
        
        # Создаем файл в памяти
        file_content = io.BytesIO(cached_doc["content"].encode('utf-8', errors='ignore'))
        filename_base = parsed_url.netloc or "scraped_page"
        filename = "".join(c if c.isalnum() or c in ('-', '_', '.') else '_' for c in filename_base) + "_single_cached.txt"
        
        # Создаем инлайн кнопки для действий с документом
        inline_kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🤖 Спросить ИИ о документе", callback_data="ask_ai")],
            [InlineKeyboardButton(text="📝 Получить конспект документа", callback_data="get_summary")]
        ])
        
        input_file = BufferedInputFile(file_content.getvalue(), filename=filename)
        
        # Отправляем результат с пометкой о кэше
        result_caption = (
            f"📄 <b>Текст со страницы:</b> <code>{url}</code>\n\n"
            f"📊 <b>Статистика:</b>\n"
            f"✅ Обработано страниц: <b>1</b>\n"
            f"📦 Размер данных: <b>{len(cached_doc['content']) // 1024} Кб</b>\n"
            f"🔄 <i>Данные получены из кэша</i>\n\n"
            f"👇 <b>Выберите действие:</b>"
        )
        
        await callback_query.message.answer_document(
            input_file,
            caption=result_caption,
            reply_markup=inline_kb,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True
        )
        
        logger.info(f"Sent cached single page content for {url} to user {user_id}")
        return

    # Если документ не найден в кэше, выполняем обход
    # Создаем стартовое сообщение
    start_message = (
        f"🌐 <b>Начинаю обработку страницы:</b> <code>{parsed_url.netloc}</code>\n\n"
        f"🔍 Подготовка к скрейпингу...\n"
        f"⚙️ Режим: <b>Одна страница</b>\n\n"
        f"⏳ Пожалуйста, подождите..."
    )
    status_message = await callback_query.message.answer(start_message, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
    
    try:
        # Используем функцию crawl_website с max_pages=1 для обработки только одной страницы
        scraped_text, pages_count = await crawl_website(url, max_pages=1, status_message=status_message)

        if scraped_text.startswith("Error:"):
            await status_message.edit_text(scraped_text, disable_web_page_preview=True)
            return
        
        if not scraped_text.strip() or pages_count == 0:
            await status_message.edit_text("Не удалось найти текстовый контент на указанной странице.", disable_web_page_preview=True)
            return
        
        # Сохраняем документ в кэш
        document_id = cache_document(url, scraped_text, pages_count, is_single_page=True)
        
        # Сохраняем текст документа и ID в состоянии с флагом одиночной страницы
        await state.update_data(
            document_text=scraped_text, 
            cached_document_id=document_id,
            is_single_page=True
        )
        
        # Создаем файл в памяти
        file_content = io.BytesIO(scraped_text.encode('utf-8', errors='ignore'))
        filename_base = parsed_url.netloc or "scraped_page"
        filename = "".join(c if c.isalnum() or c in ('-', '_', '.') else '_' for c in filename_base) + "_single.txt"
        
        # Создаем инлайн кнопки для действий с документом
        inline_kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🤖 Спросить ИИ о документе", callback_data="ask_ai")],
            [InlineKeyboardButton(text="📝 Получить конспект документа", callback_data="get_summary")]
        ])
        
        input_file = BufferedInputFile(file_content.getvalue(), filename=filename)
        
        # Отправляем результат
        result_caption = (
            f"📄 <b>Текст со страницы:</b> <code>{url}</code>\n\n"
            f"📊 <b>Статистика обработки:</b>\n"
            f"✅ Обработано страниц: <b>1</b>\n"
            f"📦 Размер данных: <b>{len(scraped_text) // 1024} Кб</b>\n\n"
            f"👇 <b>Выберите действие:</b>"
        )
        
        await callback_query.message.answer_document(
            input_file,
            caption=result_caption,
            reply_markup=inline_kb,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True
        )
        
        # Удаляем статусное сообщение
        await status_message.delete()
        
        logger.info(f"Sent single page content for {url} to user {user_id}")
        
    except Exception as e:
        logger.exception(f"Error processing single page crawl for URL {url} from user {user_id}: {e}")
        await status_message.edit_text(f"❌ Произошла непредвиденная ошибка при обработке вашего запроса: {e}", disable_web_page_preview=True)


@dp.callback_query(lambda c: c.data == 'crawl_full')
async def process_full_crawl(callback_query: types.CallbackQuery, state: FSMContext):
    """Обработчик выбора полного обхода сайта."""
    user_id = callback_query.from_user.id
    
    # Сразу отвечаем на callback query, чтобы избежать таймаута
    await callback_query.answer()
    
    # Получаем URL из состояния
    data = await state.get_data()
    url = data.get("url")
    
    if not url:
        await callback_query.message.answer("Ошибка: URL не найден. Пожалуйста, отправьте URL заново.")
        return

    parsed_initial_url = urlparse(url)
    
    # Проверяем кэш для этого URL (с флагом is_single_page=False)
    cached_doc = get_cached_document(url, is_single_page=False)
    
    # Если документ найден в кэше
    if cached_doc:
        logger.info(f"Using cached document for URL {url} (full crawl)")
        
        # Сохраняем текст документа в состоянии с флагом полного обхода
        await state.update_data(
            document_text=cached_doc["content"], 
            cached_document_id=cached_doc["id"],
            is_single_page=False
        )
        
        # Создаем файл в памяти
        file_content = io.BytesIO(cached_doc["content"].encode('utf-8', errors='ignore'))
        filename_base = parsed_initial_url.netloc or "scraped_site"
        filename = "".join(c if c.isalnum() or c in ('-', '_', '.') else '_' for c in filename_base) + "_full_cached.txt"
        
        # Создаем инлайн кнопки для действий с документом
        inline_kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🤖 Спросить ИИ о документе", callback_data="ask_ai")],
            [InlineKeyboardButton(text="📝 Получить конспект документа", callback_data="get_summary")]
        ])
        
        input_file = BufferedInputFile(file_content.getvalue(), filename=filename)
        
        # Отправляем результат с пометкой о кэше
        result_caption = (
            f"📄 <b>Текст со страниц сайта:</b> <code>{parsed_initial_url.netloc}</code>\n\n"
            f"📊 <b>Статистика:</b>\n"
            f"✅ Обработано страниц: <b>{cached_doc['pages_processed']}</b>\n"
            f"📦 Размер данных: <b>{len(cached_doc['content']) // 1024} Кб</b>\n"
            f"🔄 <i>Данные получены из кэша</i>\n\n"
            f"👇 <b>Выберите действие:</b>"
        )
        
        await callback_query.message.answer_document(
            input_file,
            caption=result_caption,
            reply_markup=inline_kb,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True
        )
        
        logger.info(f"Sent cached full crawl content for {url} to user {user_id}")
        return
    
    # Если документ не найден в кэше, выполняем обход
    # Создаем стартовое сообщение
    start_message = (
        f"🌐 <b>Начинаю обработку сайта:</b> <code>{parsed_initial_url.netloc}</code>\n\n"
        f"🔍 Подготовка к скрейпингу...\n"
        f"⚙️ Максимум страниц: <b>1000</b>\n"
        f"🕸️ Тип обхода: <b>По внутренним ссылкам</b>\n\n"
        f"⏳ Пожалуйста, подождите..."
    )
    status_message = await callback_query.message.answer(start_message, parse_mode=ParseMode.HTML, disable_web_page_preview=True)

    try:
        scraped_text, pages_count = await crawl_website(url, max_pages=1000, status_message=status_message)

        if scraped_text.startswith("Error:"):
             await status_message.edit_text(scraped_text, disable_web_page_preview=True)
             return

        if not scraped_text.strip() or pages_count == 0:
            await status_message.edit_text("Не удалось найти текстовый контент или доступные страницы для обхода.", disable_web_page_preview=True)
            return

        # Сохраняем документ в кэш
        document_id = cache_document(url, scraped_text, pages_count, is_single_page=False)
        
        # Сохраняем текст документа и ID в состоянии с флагом полного обхода
        await state.update_data(
            document_text=scraped_text, 
            cached_document_id=document_id,
            is_single_page=False
        )

        # Create a file in memory
        file_content = io.BytesIO(scraped_text.encode('utf-8', errors='ignore'))
        # Sanitize filename
        filename_base = parsed_initial_url.netloc or "scraped_site"
        filename = "".join(c if c.isalnum() or c in ('-', '_', '.') else '_' for c in filename_base) + "_full.txt"

        # Создаем инлайн кнопки "Спросить ИИ" и "Конспект"
        inline_kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🤖 Спросить ИИ о документе", callback_data="ask_ai")],
            [InlineKeyboardButton(text="📝 Получить конспект документа", callback_data="get_summary")]
        ])

        input_file = BufferedInputFile(file_content.getvalue(), filename=filename)

        # Отправляем сообщение с результатами
        result_caption = (
            f"📄 <b>Текст со страниц сайта:</b> <code>{parsed_initial_url.netloc}</code>\n\n"
            f"📊 <b>Статистика обработки:</b>\n"
            f"✅ Обработано страниц: <b>{pages_count}</b>\n"
            f"📦 Размер данных: <b>{len(scraped_text) // 1024} Кб</b>\n\n"
            f"👇 <b>Выберите действие:</b>"
        )

        await callback_query.message.answer_document(
            input_file, 
            caption=result_caption,
            reply_markup=inline_kb,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True
        )
        
        # Удаляем статусное сообщение
        await status_message.delete()
        
        logger.info(f"Sent crawled content for {url} ({pages_count} pages) to user {user_id}")

    except Exception as e:
        logger.exception(f"Error handling full crawl for URL {url} from user {user_id}: {e}")
        await status_message.edit_text(f"❌ Произошла непредвиденная ошибка при обработке вашего запроса: {e}", disable_web_page_preview=True)

@dp.message(Command("cleanup_cache"))
async def clear_cache_command(message: Message):
    """
    Команда для очистки старого кэша (доступна только администраторам).
    Удаляет записи кэша, которые не использовались более 30 дней.
    """
    # Проверяем, является ли пользователь администратором
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("⚠️ У вас нет прав для выполнения этой команды.")
        return
    
    try:
        # Удаляем старые записи кэша (старше 30 дней)
        removed_count = cleanup_old_cache(days_threshold=30)
        
        # Отправляем информацию о результате очистки
        await message.answer(
            f"✅ Очистка кэша выполнена успешно!\n\n"
            f"🗑 Удалено записей: <b>{removed_count}</b>", 
            parse_mode=ParseMode.HTML
        )
        logger.info(f"Cache cleanup completed by admin {message.from_user.id}, removed {removed_count} entries")
    except Exception as e:
        await message.answer(f"❌ Ошибка при очистке кэша: {e}")
        logger.exception(f"Error during cache cleanup: {e}")


@dp.message(Command("cache_stats"))
async def cache_stats_command(message: Message):
    """
    Команда для получения статистики кэша (доступна только администраторам).
    """
    # Проверяем, является ли пользователь администратором
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("⚠️ У вас нет прав для выполнения этой команды.")
        return
    
    try:
        # Получаем статистику кэша
        stats = cache_stats()
        
        # Если в кэше есть записи, показываем подробную статистику
        if stats["docs_count"] > 0:
            # Формируем текст статистики
            stats_text = (
                f"📊 <b>Статистика кэша:</b>\n\n"
                f"📑 <b>Документы:</b>\n"
                f"- Всего документов: <b>{stats['docs_count']}</b>\n"
                f"  • Одиночных страниц: <b>{stats['single_page_docs']}</b>\n"
                f"  • Полных обходов: <b>{stats['full_crawl_docs']}</b>\n"
                f"- Всего конспектов: <b>{stats['summaries_count']}</b>\n"
                f"  • Для одиночных страниц: <b>{stats['single_page_summaries']}</b>\n"
                f"  • Для полных обходов: <b>{stats['full_crawl_summaries']}</b>\n"
                f"- Средний размер документа: <b>{stats['avg_doc_size_kb']:.1f} Кб</b>\n\n"
            )
            
            # Добавляем информацию о самых популярных документах, если они есть
            if stats["most_popular_doc"]:
                stats_text += (
                    f"🔍 <b>Самый популярный документ:</b>\n"
                    f"- URL: <code>{stats['most_popular_doc'].url[:50]}...</code>\n"
                    f"- Просмотров: <b>{stats['most_popular_doc'].access_count}</b>\n"
                    f"- Тип: <b>{'Одиночная страница' if stats['most_popular_doc'].is_single_page else 'Полный обход'}</b>\n"
                    f"- Размер: <b>{len(stats['most_popular_doc'].content) // 1024} Кб</b>\n\n"
                )
            
            # Добавляем информацию о популярной одиночной странице, если есть
            if stats["most_popular_page"]:
                stats_text += (
                    f"📄 <b>Популярная одиночная страница:</b>\n"
                    f"- URL: <code>{stats['most_popular_page'].url[:50]}...</code>\n"
                    f"- Просмотров: <b>{stats['most_popular_page'].access_count}</b>\n\n"
                )
            
            # Добавляем информацию о популярном полном обходе, если есть
            if stats["most_popular_full"]:
                stats_text += (
                    f"🌐 <b>Популярный полный обход:</b>\n"
                    f"- URL: <code>{stats['most_popular_full'].url[:50]}...</code>\n"
                    f"- Просмотров: <b>{stats['most_popular_full'].access_count}</b>\n"
                    f"- Обработано страниц: <b>{stats['most_popular_full'].pages_processed}</b>\n\n"
                )
            
            # Добавляем информацию о времени создания
            if stats["oldest_doc_date"] and stats["newest_doc_date"]:
                stats_text += (
                    f"⏱️ <b>Временная статистика:</b>\n"
                    f"- Самый старый документ: <b>{stats['oldest_doc_date'].strftime('%d.%m.%Y %H:%M')}</b>\n"
                    f"- Самый новый документ: <b>{stats['newest_doc_date'].strftime('%d.%m.%Y %H:%M')}</b>\n\n"
                )
            
            stats_text += f"🕒 <i>Данные собраны: {datetime.datetime.now().strftime('%d.%m.%Y %H:%M:%S')}</i>"
        else:
            stats_text = "📊 <b>Статистика кэша:</b>\n\nКэш пуст."
        
        # Отправляем статистику
        await message.answer(stats_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
        logger.info(f"Cache stats requested by admin {message.from_user.id}")
    except Exception as e:
        await message.answer(f"❌ Ошибка при получении статистики кэша: {e}")
        logger.exception(f"Error getting cache stats: {e}")

async def main() -> None:
    """Initializes and starts the bot."""
    # Initialize database
    init_db()

    # Initialize Bot with default session settings.
    # No custom session or SSL context passed.
    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML)
    )

    # Run bot polling using default session management.
    await dp.start_polling(bot)


if __name__ == '__main__':
    # Ensure the main function handles potential exceptions during session creation/closing
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot stopped!")
    except Exception as e:
        logger.critical(f"Critical error during bot execution: {e}", exc_info=True)
