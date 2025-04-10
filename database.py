from sqlalchemy import Column, Integer, String, Text, Boolean, DateTime, ForeignKey, create_engine, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
import datetime
import hashlib

Base = declarative_base()

class User(Base):
    __tablename__ = 'users'
    
    id = Column(Integer, primary_key=True)
    telegram_id = Column(Integer, unique=True, index=True)
    username = Column(String(255), nullable=True)
    first_name = Column(String(255), nullable=True)
    sessions = relationship("AISession", back_populates="user", cascade="all, delete-orphan")
    
class AISession(Base):
    __tablename__ = 'ai_sessions'
    
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id'))
    document_text = Column(Text, nullable=False)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    last_activity = Column(DateTime, default=datetime.datetime.utcnow)
    is_active = Column(Boolean, default=True)
    request_count = Column(Integer, default=0)
    
    user = relationship("User", back_populates="sessions")
    messages = relationship("Message", back_populates="session", cascade="all, delete-orphan")

class Message(Base):
    __tablename__ = 'messages'
    
    id = Column(Integer, primary_key=True)
    session_id = Column(Integer, ForeignKey('ai_sessions.id'))
    role = Column(String(20), nullable=False)  # 'user' or 'assistant'
    content = Column(Text, nullable=False)
    timestamp = Column(DateTime, default=datetime.datetime.utcnow)
    
    session = relationship("AISession", back_populates="messages")

class CachedDocument(Base):
    __tablename__ = 'cached_documents'
    
    id = Column(Integer, primary_key=True)
    url = Column(String(1024), nullable=False, index=True)
    url_hash = Column(String(64), nullable=False, unique=True, index=True)  # SHA-256 хеш URL для быстрого поиска
    content = Column(Text, nullable=False)  # Сохраненный текст документа
    pages_processed = Column(Integer, default=0)  # Количество обработанных страниц
    is_single_page = Column(Boolean, default=False)  # Флаг одиночной страницы или полного обхода
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    last_accessed = Column(DateTime, default=datetime.datetime.utcnow)
    access_count = Column(Integer, default=1)  # Счетчик использования кеша
    
    # Отношение один-к-многим с конспектами
    summaries = relationship("CachedSummary", back_populates="document", cascade="all, delete-orphan")

class CachedSummary(Base):
    __tablename__ = 'cached_summaries'
    
    id = Column(Integer, primary_key=True)
    document_id = Column(Integer, ForeignKey('cached_documents.id'))
    content = Column(Text, nullable=False)  # Текст конспекта
    is_single_page = Column(Boolean, default=False)  # Флаг, указывающий тип обхода (одна страница или полный)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    last_accessed = Column(DateTime, default=datetime.datetime.utcnow)
    access_count = Column(Integer, default=1)  # Счетчик использования конспекта
    
    # Отношение многие-к-одному с документом
    document = relationship("CachedDocument", back_populates="summaries")

# Создание соединения с БД и сессии
DATABASE_URL = "sqlite:///ai_chat_sessions.db"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def init_db():
    Base.metadata.create_all(bind=engine)

def get_db():
    db = SessionLocal()
    try:
        return db
    finally:
        db.close()

def get_url_hash(url):
    """Создает уникальный хеш URL для более быстрого поиска в кэше"""
    return hashlib.sha256(url.encode('utf-8')).hexdigest()

def cache_document(url, content, pages_processed, is_single_page=False):
    """Сохраняет документ в кэш"""
    db = get_db()
    url_hash = get_url_hash(url)
    
    # Проверяем, существует ли уже документ в кэше с таким же типом обхода
    existing_doc = db.query(CachedDocument).filter(
        CachedDocument.url_hash == url_hash,
        CachedDocument.is_single_page == is_single_page
    ).first()
    
    if existing_doc:
        # Обновляем существующий документ
        existing_doc.content = content
        existing_doc.pages_processed = pages_processed
        existing_doc.last_accessed = datetime.datetime.now()
        existing_doc.access_count += 1
        db.commit()
        return existing_doc.id
    
    # Проверяем, существует ли документ с другим типом обхода
    # Если да, создаем новый с другим хешем для уникальности
    has_other_type = db.query(CachedDocument).filter(
        CachedDocument.url_hash == url_hash,
        CachedDocument.is_single_page != is_single_page
    ).first()
    
    if has_other_type:
        # Создаем модифицированный хеш для уникальности
        unique_key = f"{url}_{is_single_page}"
        url_hash = get_url_hash(unique_key)
    
    # Создаем новый документ в кэше
    new_doc = CachedDocument(
        url=url,
        url_hash=url_hash,
        content=content,
        pages_processed=pages_processed,
        is_single_page=is_single_page,
        created_at=datetime.datetime.now(),
        last_accessed=datetime.datetime.now()
    )
    
    db.add(new_doc)
    db.commit()
    db.refresh(new_doc)
    return new_doc.id

def get_cached_document(url, is_single_page=None):
    """
    Получает документ из кэша по URL и типу обхода
    
    Args:
        url (str): URL документа
        is_single_page (bool, optional): Если True - ищем только одиночную страницу,
                                        если False - только полный обход,
                                        если None - любой тип
    """
    db = get_db()
    url_hash = get_url_hash(url)
    
    # Если указан тип обхода, пробуем сначала найти по основному хешу с фильтром типа
    if is_single_page is not None:
        # Строим базовый запрос
        query = db.query(CachedDocument).filter(
            CachedDocument.url_hash == url_hash,
            CachedDocument.is_single_page == is_single_page
        )
        
        # Выполняем запрос
        cached_doc = query.first()
        
        # Если не нашли, пробуем искать по модифицированному хешу
        if not cached_doc:
            unique_key = f"{url}_{is_single_page}"
            modified_url_hash = get_url_hash(unique_key)
            
            # Строим запрос с модифицированным хешем
            query = db.query(CachedDocument).filter(
                CachedDocument.url_hash == modified_url_hash,
                CachedDocument.is_single_page == is_single_page
            )
            
            # Выполняем запрос
            cached_doc = query.first()
    else:
        # Если тип не указан, ищем по основному хешу
        cached_doc = db.query(CachedDocument).filter(
            CachedDocument.url_hash == url_hash
        ).first()
    
    if cached_doc:
        # Обновляем статистику доступа
        cached_doc.last_accessed = datetime.datetime.now()
        cached_doc.access_count += 1
        db.commit()
        
        return {
            "id": cached_doc.id,
            "content": cached_doc.content,
            "pages_processed": cached_doc.pages_processed,
            "is_single_page": cached_doc.is_single_page
        }
    
    return None

def cache_summary(document_id, summary_content, is_single_page=False):
    """Сохраняет конспект документа в кэш"""
    db = get_db()
    
    # Проверяем, существует ли уже конспект для этого документа с таким же типом (одна страница/полный обход)
    existing_summary = db.query(CachedSummary).filter(
        CachedSummary.document_id == document_id,
        CachedSummary.is_single_page == is_single_page
    ).first()
    
    if existing_summary:
        # Обновляем существующий конспект
        existing_summary.content = summary_content
        existing_summary.last_accessed = datetime.datetime.now()
        existing_summary.access_count += 1
        db.commit()
        return existing_summary.id
    
    # Создаем новый конспект
    new_summary = CachedSummary(
        document_id=document_id,
        content=summary_content,
        is_single_page=is_single_page,
        created_at=datetime.datetime.now(),
        last_accessed=datetime.datetime.now()
    )
    
    db.add(new_summary)
    db.commit()
    db.refresh(new_summary)
    return new_summary.id

def get_cached_summary(document_id, is_single_page=False):
    """Получает конспект из кэша по ID документа и типу обхода"""
    db = get_db()
    
    cached_summary = db.query(CachedSummary).filter(
        CachedSummary.document_id == document_id,
        CachedSummary.is_single_page == is_single_page
    ).first()
    
    if cached_summary:
        # Обновляем статистику доступа
        cached_summary.last_accessed = datetime.datetime.now()
        cached_summary.access_count += 1
        db.commit()
        
        return cached_summary.content
    
    return None

def cleanup_old_cache(days_threshold=30):
    """Удаляет старые записи из кэша, которые не использовались более N дней"""
    db = get_db()
    cutoff_date = datetime.datetime.now() - datetime.timedelta(days=days_threshold)
    
    # Находим документы, которые не использовались больше порогового значения
    old_documents = db.query(CachedDocument).filter(CachedDocument.last_accessed < cutoff_date).all()
    
    # Удаляем найденные документы (связанные конспекты удалятся каскадно)
    for doc in old_documents:
        db.delete(doc)
    
    db.commit()
    return len(old_documents)

def cache_stats(db=None):
    """Получает детальную статистику кэша"""
    if db is None:
        db = get_db()
    
    # Статистика по документам
    docs_count = db.query(CachedDocument).count()
    
    # Статистика по типам документов
    single_page_docs = db.query(CachedDocument).filter(CachedDocument.is_single_page == True).count()
    full_crawl_docs = db.query(CachedDocument).filter(CachedDocument.is_single_page == False).count()
    
    # Статистика по конспектам
    summaries_count = db.query(CachedSummary).count()
    
    # Статистика по типам конспектов
    single_page_summaries = db.query(CachedSummary).filter(CachedSummary.is_single_page == True).count()
    full_crawl_summaries = db.query(CachedSummary).filter(CachedSummary.is_single_page == False).count()
    
    # Статистика по времени создания
    oldest_doc = db.query(CachedDocument.created_at).order_by(CachedDocument.created_at).first()
    oldest_doc_date = oldest_doc[0] if oldest_doc else None
    
    newest_doc = db.query(CachedDocument.created_at).order_by(CachedDocument.created_at.desc()).first()
    newest_doc_date = newest_doc[0] if newest_doc else None
    
    # Вычисляем средний размер документа
    avg_doc_size = db.query(func.avg(func.length(CachedDocument.content))).scalar() or 0
    avg_doc_size_kb = avg_doc_size / 1024
    
    # Самый популярный документ
    most_popular_doc = db.query(CachedDocument).order_by(CachedDocument.access_count.desc()).first()
    
    # Самая популярная страница
    most_popular_page = db.query(CachedDocument).filter(
        CachedDocument.is_single_page == True
    ).order_by(CachedDocument.access_count.desc()).first()
    
    # Самый популярный полный обход
    most_popular_full = db.query(CachedDocument).filter(
        CachedDocument.is_single_page == False
    ).order_by(CachedDocument.access_count.desc()).first()
    
    # Формируем и возвращаем статистику
    return {
        "docs_count": docs_count,
        "single_page_docs": single_page_docs,
        "full_crawl_docs": full_crawl_docs,
        "summaries_count": summaries_count,
        "single_page_summaries": single_page_summaries,
        "full_crawl_summaries": full_crawl_summaries,
        "oldest_doc_date": oldest_doc_date,
        "newest_doc_date": newest_doc_date,
        "avg_doc_size_kb": avg_doc_size_kb,
        "most_popular_doc": most_popular_doc,
        "most_popular_page": most_popular_page,
        "most_popular_full": most_popular_full
    } 