import time
import logging
from collections import defaultdict
from config import RATE_LIMIT, BAN_DURATION

logger = logging.getLogger(__name__)

class RateLimiter:
    """
    Класс для ограничения количества запросов от пользователей
    для защиты от DoS и DDoS атак.
    """
    def __init__(self):
        # Для каждого типа запросов храним словарь {user_id: [(timestamp, count), ...]}
        self.request_history = {
            "url_requests": defaultdict(list),
            "ai_requests": defaultdict(list),
            "general_requests": defaultdict(list)
        }
        
        # Словарь для хранения времени блокировки пользователя {user_id: unban_time}
        self.banned_users = {}
        
    def is_user_banned(self, user_id):
        """Проверяет, заблокирован ли пользователь."""
        if user_id in self.banned_users:
            ban_until = self.banned_users[user_id]
            current_time = time.time()
            
            if current_time < ban_until:
                # Пользователь все еще заблокирован
                remaining_time = int(ban_until - current_time)
                return True, remaining_time
            else:
                # Время блокировки истекло
                del self.banned_users[user_id]
                return False, 0
        
        return False, 0
    
    def add_request(self, user_id, request_type="general_requests"):
        """
        Добавляет запрос в историю пользователя.
        
        Args:
            user_id: ID пользователя
            request_type: Тип запроса ('url_requests', 'ai_requests', 'general_requests')
            
        Returns:
            tuple: (is_allowed, reason, remaining_time)
                - is_allowed: True если запрос разрешен, False если отклонен
                - reason: Причина отклонения или None
                - remaining_time: Оставшееся время до разблокировки (в секундах) или 0
        """
        # Проверяем, не заблокирован ли пользователь
        is_banned, remaining_time = self.is_user_banned(user_id)
        if is_banned:
            return False, "banned", remaining_time
        
        # Добавляем общий запрос в любом случае
        current_time = time.time()
        self._add_to_history(user_id, "general_requests", current_time)
        
        # Для specific-запросов тоже добавляем запись, если тип не general
        if request_type != "general_requests":
            self._add_to_history(user_id, request_type, current_time)
        
        # Проверяем лимиты по всем типам запросов
        for req_type in ["general_requests", request_type]:
            if req_type == "general_requests" and request_type != "general_requests":
                continue  # Уже проверили general_requests
                
            is_allowed, reason = self._check_limit(user_id, req_type)
            if not is_allowed:
                # Блокируем пользователя, если он превысил лимит
                self._ban_user(user_id)
                logger.warning(f"User {user_id} banned for {BAN_DURATION} seconds for exceeding {req_type} limit")
                return False, reason, BAN_DURATION
        
        return True, None, 0
    
    def _add_to_history(self, user_id, request_type, timestamp):
        """Добавляет запрос в историю."""
        self.request_history[request_type][user_id].append(timestamp)
        
        # Очищаем устаревшие записи
        self._cleanup_history(user_id, request_type)
    
    def _cleanup_history(self, user_id, request_type):
        """Удаляет устаревшие записи из истории."""
        if request_type not in RATE_LIMIT:
            return
            
        period = RATE_LIMIT[request_type]["period"]
        current_time = time.time()
        cutoff_time = current_time - period
        
        # Оставляем только записи за период period
        self.request_history[request_type][user_id] = [
            ts for ts in self.request_history[request_type][user_id]
            if ts > cutoff_time
        ]
    
    def _check_limit(self, user_id, request_type):
        """Проверяет, не превышает ли пользователь лимит запросов."""
        if request_type not in RATE_LIMIT:
            return True, None
            
        period = RATE_LIMIT[request_type]["period"]
        max_count = RATE_LIMIT[request_type]["count"]
        
        # Очищаем устаревшие записи
        self._cleanup_history(user_id, request_type)
        
        # Проверяем количество запросов
        count = len(self.request_history[request_type][user_id])
        if count > max_count:
            return False, f"rate_limit_exceeded_{request_type}"
            
        return True, None
    
    def _ban_user(self, user_id):
        """Временно блокирует пользователя."""
        self.banned_users[user_id] = time.time() + BAN_DURATION
        
    def get_stats(self):
        """Возвращает статистику по пользователям и запросам."""
        stats = {
            "total_users": len(set().union(
                *[self.request_history[req_type].keys() for req_type in self.request_history]
            )),
            "banned_users": len(self.banned_users),
            "requests_by_type": {
                req_type: sum(len(requests) for requests in users.values())
                for req_type, users in self.request_history.items()
            },
            "active_users": {
                req_type: len(users)
                for req_type, users in self.request_history.items()
            }
        }
        return stats

# Глобальный экземпляр лимитера
rate_limiter = RateLimiter() 