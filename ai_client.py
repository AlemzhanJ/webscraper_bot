from openai import AsyncOpenAI
import logging

logger = logging.getLogger(__name__)

class AIClient:
    def __init__(self, api_key, base_url="https://generativelanguage.googleapis.com/v1beta/openai/", model="gemini-2.0-flash"):
        """
        Инициализирует клиент для работы с моделями AI через Gemini API.
        """
        try:
            # Правильный URL должен оканчиваться на /openai/
            self.client = AsyncOpenAI(
                api_key=api_key,
                base_url=base_url
            )
            self.model = model
            logger.info(f"AI client initialized with model {model}")
        except Exception as e:
            logger.error(f"Error initializing AI client: {e}")
            raise
    
    async def get_completion(self, messages, system_prompt=None):
        """
        Get completion from the AI API.
        
        Args:
            messages (list): List of message dictionaries with 'role' and 'content'
            system_prompt (str, optional): System prompt to use
            
        Returns:
            str: The model's response
        """
        # Add system message if provided
        formatted_messages = []
        if system_prompt:
            formatted_messages.append({"role": "system", "content": system_prompt})
        
        # Add the conversation messages
        formatted_messages.extend(messages)
        
        try:
            logger.info(f"Sending request to AI model with {len(formatted_messages)} messages")
            completion = await self.client.chat.completions.create(
                model=self.model,
                messages=formatted_messages,
                n=1
            )
            
            # Проверяем, что ответ не None и что у него есть choices
            if completion and hasattr(completion, 'choices') and completion.choices:
                # Проверяем, что у первого choice есть message с content
                if hasattr(completion.choices[0], 'message') and completion.choices[0].message and hasattr(completion.choices[0].message, 'content'):
                    return completion.choices[0].message.content
                else:
                    logger.error("Invalid response format from AI API")
                    return "Ошибка при получении ответа: неверный формат ответа от API"
            else:
                logger.error("Empty response from AI API")
                return "Ошибка при получении ответа: пустой ответ от API"
        except Exception as e:
            # Логгируем ошибку и возвращаем текст ошибки
            logger.exception(f"Error getting completion: {e}")
            return f"Ошибка при получении ответа от ИИ: {str(e)}" 