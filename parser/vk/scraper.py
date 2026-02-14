import os
import time
import re
import json
import vk_api
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv

# --- НАСТРОЙКИ ---
load_dotenv()
VK_TOKEN = os.getenv("VK_TOKEN")
INPUT_FILENAME = "input_vk.json"

# Лимиты
GROUPS_LIMIT_PER_QUERY = 20   # Групп на 1 запрос
POSTS_PER_GROUP = 30          # Постов со стены
DAYS_TO_CHECK = 365           # Глубина (1 год)

def extract_phone(text):
    if not text: return None
    pattern = r'(?:\+7|8|7)[\s\-]?\(?(\d{3})\)?[\s\-]?(\d{3})[\s\-]?(\d{2})[\s\-]?(\d{2})'
    match = re.search(pattern, text)
    if match: return match.group(0)
    return None

def main():
    print("🚀 Запуск скрапера (с авторами и названиями групп)...")
    
    if not VK_TOKEN:
        print("❌ Ошибка: Токен не найден в .env")
        return

    # 1. Читаем JSON
    if not os.path.exists(INPUT_FILENAME):
        print(f"❌ Файл {INPUT_FILENAME} не найден!")
        return

    with open(INPUT_FILENAME, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    queries = [item['query'] for item in data['queries']]
    print(f"📂 Загружено {len(queries)} запросов.")

    try:
        vk_session = vk_api.VkApi(token=VK_TOKEN)
        vk = vk_session.get_api()
    except Exception as e:
        print(f"❌ Ошибка авторизации ВК: {e}")
        return

    all_posts = []
    seen_post_links = set() 
    
    # --- ГЛАВНЫЙ ЦИКЛ ---
    for q_idx, query in enumerate(queries):
        print(f"\n🔎 [{q_idx+1}/{len(queries)}] Запрос: '{query}'")
        
        try:
            # 1. Ищем группы
            clean_query = query.replace('#', '').strip()
            groups = vk.groups.search(q=clean_query, count=GROUPS_LIMIT_PER_QUERY, sort=0)['items']
            
            if not groups:
                print("   Групп не найдено.")
                continue

            # 2. Проходим по каждой группе
            for g in groups:
                if g['is_closed'] == 1: continue 
                
                group_id = g['id']
                group_name = g['name'] # <-- ВОТ НАЗВАНИЕ ГРУППЫ
                
                try:
                    # Скачиваем стену
                    posts = vk.wall.get(owner_id=f"-{group_id}", count=POSTS_PER_GROUP)['items']
                    
                    for post in posts:
                        # Фильтр по дате
                        post_date = datetime.fromtimestamp(post['date'])
                        if post_date < datetime.now() - timedelta(days=DAYS_TO_CHECK):
                            continue

                        # Текст + Репост
                        text = post.get('text', '')
                        if 'copy_history' in post and len(post['copy_history']) > 0:
                            text += "\n--- REPOST ---\n" + post['copy_history'][0].get('text', '')
                        
                        if not text.strip(): continue

                        post_link = f"https://vk.com/wall-{group_id}_{post['id']}"
                        
                        if post_link in seen_post_links: continue
                        seen_post_links.add(post_link)

                        # --- ЛОГИКА ОПРЕДЕЛЕНИЯ АВТОРА ---
                        from_id = post.get('from_id')
                        author_link = ""
                        author_type = ""
                        
                        if from_id:
                            if from_id < 0:
                                # Отрицательный ID = писала группа
                                author_type = "Группа"
                                author_link = f"https://vk.com/public{abs(from_id)}"
                            else:
                                # Положительный ID = писал человек
                                author_type = "Человек"
                                author_link = f"https://vk.com/id{from_id}"
                        
                        # Иногда есть подпись "signer_id" (кто именно из админов запостил)
                        signer_id = post.get('signer_id')
                        signer_link = ""
                        if signer_id:
                            signer_link = f"https://vk.com/id{signer_id}"

                        # СОХРАНЯЕМ
                        all_posts.append({
                            'search_query': query,         # По какому запросу нашли
                            'group_name': group_name,      # Название группы
                            'author_type': author_type,    # Кто автор (Группа/Человек)
                            'author_link': author_link,    # Ссылка на автора
                            'signer_link': signer_link,    # Ссылка на автора (если пост от имени группы с подписью)
                            'date': post_date.strftime('%Y-%m-%d'),
                            'phone': extract_phone(text),
                            'link': post_link,
                            'text': text[:5000]
                        })
                    
                    time.sleep(0.2) 

                except Exception:
                    pass
            
        except Exception as e:
            print(f"Ошибка: {e}")

    # --- СОХРАНЕНИЕ ---
    print("\n")
    if all_posts:
        df = pd.DataFrame(all_posts)
        filename = f"vk_data_with_authors_{datetime.now().strftime('%m%d_%H%M')}.xlsx"
        # Сортируем колонки для удобства
        cols = ['date', 'city', 'phone', 'group_name', 'author_type', 'author_link', 'signer_link', 'link', 'text', 'search_query']
        # Оставляем только те колонки, которые есть в датафрейме (на случай ошибок)
        final_cols = [c for c in cols if c in df.columns]
        df = df[final_cols]
        
        df.to_excel(filename, index=False)
        print(f"✅ Готово! Собрано {len(df)} записей.")
        print(f"💾 Файл: {filename}")
    else:
        print("😔 Ничего не найдено.")

if __name__ == "__main__":
    main()