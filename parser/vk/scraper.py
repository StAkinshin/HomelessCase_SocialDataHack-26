import os
import time
import re
import vk_api
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
VK_TOKEN = os.getenv("VK_TOKEN")

if not VK_TOKEN:
    print("❌ Токен не найден в .env")
    exit()

# 1. ЗАПРОСЫ ДЛЯ ПОИСКА ГРУПП (Сообществ)
GROUP_QUERIES = [
    'Рабочий дом', 
    'Работа с проживанием', 
    'Помощь попавшим в трудную ситуацию',
    'Социальная адаптация',
    'Приют для рабочих'
]

# Сколько групп проверять по каждому запросу
GROUPS_COUNT = 20 
# Сколько постов брать со стены каждой группы
POSTS_PER_GROUP = 30

def get_phone(text):
    if not text: return None
    # Ищем номера (более гибкий regex)
    pattern = r'(?:\+7|8|7)[\s\-]?\(?(\d{3})\)?[\s\-]?(\d{3})[\s\-]?(\d{2})[\s\-]?(\d{2})'
    match = re.search(pattern, text)
    if match:
        return match.group(0)
    return None

def main():
    print("🚀 Запуск парсера ПО ГРУППАМ...")
    
    try:
        vk_session = vk_api.VkApi(token=VK_TOKEN)
        vk = vk_session.get_api()
    except Exception as e:
        print(f"Ошибка входа: {e}")
        return

    # Шаг 1: Собираем ID групп
    target_groups = []
    print("🔎 Ищем целевые сообщества...")
    
    for query in GROUP_QUERIES:
        try:
            # Ищем сообщества
            groups = vk.groups.search(q=query, count=GROUPS_COUNT, sort=0)['items']
            for g in groups:
                if g['is_closed'] == 0: # Берем только открытые группы
                    target_groups.append({
                        'id': g['id'],
                        'name': g['name'],
                        'screen_name': g['screen_name']
                    })
            print(f"   Найдено {len(groups)} групп по запросу '{query}'")
            time.sleep(0.5)
        except Exception as e:
            print(f"Ошибка поиска групп: {e}")

    # Удаляем дубликаты групп (если нашлись по разным запросам)
    # Используем словарь для уникальности по ID
    unique_groups = {g['id']: g for g in target_groups}.values()
    print(f"🎯 Всего уникальных групп для обработки: {len(unique_groups)}")

    # Шаг 2: Парсим стены этих групп
    all_posts = []
    
    print("📥 Начинаем скачивать объявления со стен...")
    for idx, group in enumerate(unique_groups):
        print(f"[{idx+1}/{len(unique_groups)}] Сканируем: {group['name']}")
        
        try:
            # wall.get получает посты со стены
            # owner_id для группы должен быть с минусом!
            posts = vk.wall.get(owner_id=f"-{group['id']}", count=POSTS_PER_GROUP)['items']
            
            for post in posts:
                text = post.get('text', '')
                
                # Если пост пустой (например, только картинка), пропускаем
                if not text: continue
                
                phone = get_phone(text)
                
                # ФИЛЬТР: Берем только если есть телефон ИЛИ слова про работу/жилье
                # Это отсеет просто картинки с котиками, если они там есть
                if not phone and "проживан" not in text.lower():
                    continue

                all_posts.append({
                    'group_name': group['name'],
                    'date': datetime.fromtimestamp(post['date']).strftime('%Y-%m-%d'),
                    'phone': phone,
                    'city': '?', # Будем искать позже
                    'link': f"https://vk.com/wall-{group['id']}_{post['id']}",
                    'text': text[:800] # Берем побольше текста
                })
            
            time.sleep(0.4) # Небольшая пауза между группами

        except Exception as e:
            print(f"   Не удалось прочитать стену группы {group['id']}: {e}")

    # Шаг 3: Сохраняем
    if all_posts:
        df = pd.DataFrame(all_posts)
        # Удаляем дубликаты по тексту объявления (часто постят одно и то же)
        df = df.drop_duplicates(subset=['text'])
        
        filename = f"groups_data_{datetime.now().strftime('%H%M')}.xlsx"
        df.to_excel(filename, index=False)
        print(f"\n✅ УСПЕХ! Собрано {len(df)} объявлений.")
        print(f"Файл: {filename}")
        print("Совет: Открой файл и отсортируй по колонке 'phone', чтобы найти сетевые дома.")
    else:
        print("Ничего не собрали. Возможно, группы закрыты или пустые.")

if __name__ == "__main__":
    main()