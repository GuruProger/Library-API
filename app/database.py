import os
from dotenv import load_dotenv
import psycopg2
from datetime import datetime
from app_model import BookModel

# Загружаем переменные окружения из файла .env
load_dotenv()


class DatabaseManager:
    def __init__(self) -> None:
        # Инициализация переменных окружения для подключения к базе данных
        self.db_name: str = os.getenv('DB_NAME')
        self.user: str = os.getenv('DB_USER')
        self.password: str = os.getenv('DB_PASSWORD')
        self.host: str = os.getenv('DB_HOST')
        self.connection: psycopg2.extensions.connection | None = None

    def connect(self) -> None:
        """Устанавливает соединение с базой данных PostgreSQL"""
        try:
            self.connection = psycopg2.connect(
                dbname=self.db_name,
                user=self.user,
                password=self.password,
                host=self.host
            )
            self.connection.autocommit = True
            print("Соединение с базой данных PostgreSQL установлено")
        except Exception as e:
            print(f"Ошибка при подключении к базе данных: {e}")

    def close(self) -> None:
        """Закрывает соединение с базой данных"""
        if self.connection:
            self.connection.close()
            print("Соединение с базой данных закрыто")

    def create_tables(self) -> None:
        """Создает таблицы в базе данных, если они не существуют"""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS authors (
                        id SERIAL PRIMARY KEY,
                        first_name VARCHAR(50) NOT NULL,
                        last_name VARCHAR(50) NOT NULL,
                        avatar BYTEA
                    );

                    CREATE TABLE IF NOT EXISTS genres (
                        id SERIAL PRIMARY KEY,
                        name VARCHAR(32) UNIQUE NOT NULL
                    );""")
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS books (
                        id SERIAL PRIMARY KEY,
                        title VARCHAR(255) UNIQUE NOT NULL,
                        price NUMERIC(7, 2) NOT NULL,
                        pages SMALLINT NOT NULL,
                        author_id INTEGER NOT NULL REFERENCES authors(id)
                    );""")
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS book_genres (
                        book_id INTEGER NOT NULL REFERENCES books(id),
                        genre_id INTEGER NOT NULL REFERENCES genres(id),
                        PRIMARY KEY (book_id, genre_id)
                    );

                    CREATE TABLE IF NOT EXISTS bookings (
                        user_id INTEGER NOT NULL,
                        book_id INTEGER NOT NULL REFERENCES books(id),
                        start_date TIMESTAMP NOT NULL,
                        end_date TIMESTAMP NOT NULL
                    );""")

            print("Таблицы созданы успешно")
        except Exception as e:
            print(f"Ошибка при создании таблиц: {e}")

    def add_book(self, book: BookModel) -> None:
        """Добавляет книгу и соответствующие данные в базу данных"""
        try:
            with self.connection.cursor() as cursor:
                # Проверка и добавление автора
                cursor.execute("""
                    SELECT id FROM authors WHERE first_name = %s AND last_name = %s;
                """, (book.first_name, book.last_name))
                author = cursor.fetchone()
                if author is None:
                    cursor.execute("""
                        INSERT INTO authors (first_name, last_name, avatar) 
                        VALUES (%s, %s, %s) RETURNING id;
                    """, (book.first_name, book.last_name, book.avatar))
                    author_id = cursor.fetchone()[0]
                else:
                    author_id = author[0]

                # Проверка и добавление жанров
                genre_ids = []
                for genre in book.genres:
                    cursor.execute("""
                        SELECT id FROM genres WHERE name = %s;
                    """, (genre,))
                    genre_result = cursor.fetchone()
                    if genre_result is None:
                        cursor.execute("""
                            INSERT INTO genres (name) VALUES (%s) RETURNING id;
                        """, (genre,))
                        genre_id = cursor.fetchone()[0]
                    else:
                        genre_id = genre_result[0]
                    genre_ids.append(genre_id)

                # Добавление книги
                cursor.execute("""
                    INSERT INTO books (title, price, pages, author_id) 
                    VALUES (%s, %s, %s, %s) RETURNING id;
                """, (book.title, book.price, book.pages, author_id))
                book_id = cursor.fetchone()[0]

                # Добавление связи книга-жанр
                for genre_id in genre_ids:
                    cursor.execute("""
                        INSERT INTO book_genres (book_id, genre_id) 
                        VALUES (%s, %s);
                    """, (book_id, genre_id))
        except Exception as e:
            print(f"Ошибка при записи книги: {e}")

    def remove_book(self, book_id: int) -> None:
        """Удаляет книгу и связанные записи из базы данных"""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("""
                    DELETE FROM book_genres WHERE book_id = %s;
                    DELETE FROM books WHERE id = %s;
                """, (book_id, book_id,))
            books_deleted = cursor.rowcount
            if books_deleted == 0:
                print('Книги с таким id не существует')
                return
            print("Книга удалена")
        except Exception as e:
            print(f"Ошибка при удалении книги: {e}")

    def get_books_filtered(self, min_price: float | int = None, max_price: float | int = None, genre_name: str = None,
                           author_first_name: str = None,
                           author_last_name: str = None) -> dict[int, dict[str, str | float | int]] | None:
        """Возвращает список книг, соответствующих фильтрам"""
        self._checking_booking_date()
        try:
            query = """
                SELECT books.id, books.title, books.price, books.pages, authors.first_name, authors.last_name
                FROM books
                JOIN book_genres ON books.id = book_genres.book_id
                JOIN genres ON book_genres.genre_id = genres.id
                JOIN authors ON books.author_id = authors.id
                WHERE 1=1"""
            params = []

            if min_price is not None and isinstance(min_price, (int, float)):
                query += " AND books.price >= %s"
                params.append(min_price)

            if max_price is not None and isinstance(max_price, (int, float)):
                query += " AND books.price <= %s"
                params.append(max_price)

            if author_first_name:
                query += " AND authors.first_name = %s"
                params.append(author_first_name)

            if author_last_name:
                query += " AND authors.last_name = %s"
                params.append(author_last_name)

            if genre_name:
                query += " AND genres.name = %s"
                params.append(genre_name)

            with self.connection.cursor() as cursor:
                cursor.execute(query, params)
                books = cursor.fetchall()
                books_list = {}
                for book in books:
                    books_list[book[0]] = {
                        "title": book[1],
                        "price": book[2],
                        "pages": book[3],
                        "first_name": book[4],
                        "last_name": book[5],
                    }
                return books_list
        except Exception as e:
            print(f"Ошибка при выборке книг с фильтрами: {e}")

    def _is_available_bookings(self, book_id: int, start_date: datetime) -> bool | None:
        """Проверяет, доступна ли книга для бронирования на указанную дату"""
        self._checking_booking_date()

        try:
            with self.connection.cursor() as cursor:
                # Проверяем не занят ли временной промежуток у других бронирований
                cursor.execute(
                    """
                    SELECT COUNT(*) FROM bookings
                    WHERE book_id = %s
                    AND (start_date < %s AND %s < end_date)
                    """, (book_id, start_date, start_date,))

                count = cursor.fetchone()[0]
                return count == 0  # True - пересечений нет
        except Exception as e:
            print(f"Ошибка при проверки бронирования: {e}")

    def add_booking(self, user_id: int, book_id: int, start_date: datetime, end_date: datetime) -> None:
        """Добавляет бронирование книги пользователем"""
        start_date = start_date.replace(microsecond=0)
        end_date = end_date.replace(microsecond=0)
        try:
            if self._is_available_bookings(book_id, start_date):
                with self.connection.cursor() as cursor:
                    cursor.execute(
                        """
                        INSERT INTO bookings (user_id, book_id, start_date, end_date)
                        VALUES (%s, %s, %s, %s)
                        """, (user_id, book_id, start_date, end_date,))
                print("Книга забронирована")

            else:
                print(f'Ошибка: книга уже забронирована на это время')
        except Exception as e:
            print(f"Ошибка при создании бронирования: {e}")

    def cancel_booking(self, user_id: int, booking_id: int) -> None:
        """Отменяет бронирование книги"""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(
                    """
                    DELETE FROM bookings WHERE book_id = %s AND user_id = %s
                    """, (booking_id, user_id,)
                )
            print("Бронирование успешно отменено")
        except Exception as e:
            print(f"Ошибка при отмене бронирования: {e}")

    def _checking_booking_date(self) -> None:
        """Снимает бронь с книг, срок бронирования которых истёк"""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(
                    """DELETE FROM bookings
                    WHERE end_date < %s;
                    """, (datetime.now(),))
        except Exception as e:
            print(f"Ошибка при снятии бронирования по времени: {e}")

    def get_active_bookings(self) -> list[tuple[int, datetime, datetime]] | None:
        """Возвращает список активных бронирований"""
        self._checking_booking_date()
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("""
                    SELECT bookings.book_id, bookings.start_date, bookings.end_date
                    FROM bookings
                    JOIN books ON bookings.book_id = books.id
                    JOIN authors ON books.author_id = authors.id
                    WHERE bookings.end_date >= %s
                    """, (datetime.now(),))

                bookings_list = cursor.fetchall()
                return bookings_list
        except Exception as e:
            print(f"Ошибка при получении активных бронирований: {e}")

    def get_genres_by_book_id(self, book_id: int) -> list | None:
        """Возвращает список жанров книги по её идентификатору"""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("""
                       SELECT g.name
                       FROM genres g
                       JOIN book_genres bg ON g.id = bg.genre_id
                       WHERE bg.book_id = %s
                       ORDER BY g.name;
                   """, (book_id,))
                results = cursor.fetchall()
                genres = [row[0] for row in results]
                return genres
        except Exception as e:
            print(f"Ошибка при получении жанра книги: {e}")

    def _remove_all_tables(self) -> None:
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("""
                        DROP TABLE IF EXISTS books CASCADE;
                        DROP TABLE IF EXISTS authors CASCADE;
                        DROP TABLE IF EXISTS bookings CASCADE;
                        DROP TABLE IF EXISTS book_genres CASCADE;
                        DROP TABLE IF EXISTS genres CASCADE;
                   """)

        except Exception as e:
            print(f"Ошибка при получении жанра книги: {e}")



# Пример использования класса
if __name__ == "__main__":
    from data_for_migration import books_list
    from datetime import timedelta

    db_manager = DatabaseManager()
    db_manager.connect()
    db_manager.create_tables()
    print('-'*255)

    # Добавление книг
    for book in books_list:
        db_manager.add_book(BookModel(*book.values()))
    print('-'*255)

    genre_name = 'Научная фантастика'
    filter_books = db_manager.get_books_filtered(genre_name=genre_name)

    if filter_books:
        print(f"\nКниги в жанре '{genre_name}': {filter_books.keys()}")
    print('-'*255)
    # Удаление книги
    db_manager.remove_book(2)

    filter_books = db_manager.get_books_filtered(genre_name=genre_name)
    if filter_books:
        print(f"\nКниги в жанре '{genre_name}': {filter_books.keys()}")
    print('-'*255)

    start_time = datetime.now()
    delta = timedelta(hours=1)

    # Бронирование книг
    db_manager.add_booking(1, 5, start_time, start_time+timedelta(hours=1))
    db_manager.add_booking(2, 5, start_time+timedelta(minutes=30), start_time+timedelta(hours=2))
    db_manager.add_booking(2, 4, start_time, start_time+timedelta(hours=1))
    # Смотрим забронированные книги
    active_bookings = db_manager.get_active_bookings()
    if active_bookings:
        print(active_bookings)
    print('-'*255)

    # Отмена бронирования
    db_manager.cancel_booking(1, 5)
    # Смотрим забронированные книги
    active_bookings = db_manager.get_active_bookings()
    if active_bookings:
        print(active_bookings)
    print('-'*255)

    # db_manager._remove_all_tables() # ПОЛНОСТЬЮ УДАЛЯЕТ ВСЕ ТАБЛИЦЫ ИЗ БАЗЫ ДАННЫХ

    db_manager.close()
