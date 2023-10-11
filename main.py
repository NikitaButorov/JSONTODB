from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.orm import relationship
import json
import datetime
from sqlalchemy_utils import create_database, database_exists
from sqlalchemy import create_engine, Column
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import declarative_base




main = Flask(__name__)
main.config['SQLALCHEMY_DATABASE_URI'] = 'mysql+pymysql://root@localhost/books'
db = SQLAlchemy()
db_uri = 'mysql+pymysql://root@localhost/books'
db.init_app(main)
if not database_exists(db_uri):
    create_database(db_uri)
engine = create_engine(db_uri)
Base = declarative_base()
Base.metadata.create_all(engine)



class Books(db.Model):
    __tablename__ = 'books'
    id = Column(db.Integer, primary_key=True, autoincrement=True)
    title = Column(db.String(50), nullable=True)
    isbn = Column(db.String(50), nullable=True)
    pageCount = Column(db.Integer, nullable=True)
    publishedDate = Column(db.DateTime, nullable=True)
    thumbnailUrl = Column(db.String(100), nullable=True)
    shortDescription = Column(db.Text, nullable=True)
    longDescription = Column(db.Text, nullable=True)
    status = Column(db.String(50), nullable=True)
    author_id = Column(db.Integer,db.ForeignKey('authors.author_id'), primary_key=True,nullable=True)
    category_id = Column(db.Integer,db.ForeignKey('categories.category_id'), primary_key=True, nullable=True)

    author = relationship('Authors', foreign_keys=[author_id], cascade="all, delete-orphan")
    category = relationship('Categories', foreign_keys=[category_id], cascade="all, delete-orphan")
    # authorbook = relationship('AuthorBooks', foreign_keys=[id])
    # bookcategory = relationship('BooksCategories', foreign_keys=[id])
    # authorbook = relationship('AuthorBooks', back_populates='book')
    # bookcategory = relationship('BooksCategories', back_populates='book')


class Authors(db.Model):
    __tablename__ = 'authors'
    author_id = Column(db.Integer, primary_key=True, autoincrement=True, nullable=True)
    author_name = Column(db.String(50), nullable=True)

    # author = relationship('AuthorBooks', back_populates='author')



class Categories(db.Model):
    __tablename__ = 'categories'
    category_id = Column(db.Integer, primary_key=True, autoincrement=True, nullable=True)
    category_name = Column(db.String(50), nullable=True)

    # category = relationship('BooksCategories', back_populates='category')


# class AuthorBooks(db.Model):
#     __tablename__ = 'authorbooks'
#     author_id = Column(db.Integer, db.ForeignKey('authors.author_id'), primary_key=True, nullable = True)
#     book_id = Column(db.Integer, db.ForeignKey('books.id') , primary_key=True, nullable=True)
#
#     book = relationship('Books', back_populates='authorbook', primaryjoin='AuthorBooks.book_id == Books.id')
#     author = relationship('Authors', back_populates='author', primaryjoin='AuthorBooks.author_id == Authors.author_id')
#
# class BooksCategories(db.Model):
#     book_id= Column(db.Integer,db.ForeignKey('books.id'), primary_key=True,nullable=True)
#     category_id=Column(db.Integer,db.ForeignKey('categories.category_id'),primary_key=True,nullable=True)
#
#     category = relationship('Categories', back_populates='category')
#     book = relationship('Books', back_populates='bookcategory')




@main.route('/')
def insert_data_from_json_books(file_name):
    Session = sessionmaker(bind=engine)
    session = Session()
    file_path = file_name
    with open('amazon.books.json', 'r') as json_file:
        data = json.load(json_file)

        for item in data:
            published_date_str = item.get('publishedDate')
            author_name = item.get('authors', [])
            category_name = item.get('categories', [])
            short_description = item.get('shortDescription', None)
            long_description = item.get('longDescription', None)
            thumbnail_Url = item.get('thumbnailUrl', None)
            is_bn = item.get('isbn', None)

            if published_date_str:
                try:
                    published_date = datetime.datetime.strptime(published_date_str['$date'], '%Y-%m-%dT%H:%M:%S.%f%z')
                except ValueError:
                    published_date = None
            if author_name:
                author_name = author_name[0]
            if category_name:
                category_name = category_name[0]

            if author_name and isinstance(author_name, str):
                author = session.query(Authors).filter_by(author_name=author_name).first()
            else:
                author = None

            if category_name and isinstance(category_name, str):
                category = session.query(Categories).filter_by(category_name=category_name).first()
            else:
                category = None

            existing_book = session.query(Books).filter_by(title=item['title']).first()

            book = Books(
                title=item['title'],
                isbn=is_bn,
                pageCount=item['pageCount'],
                thumbnailUrl=thumbnail_Url,
                shortDescription=short_description,
                longDescription=long_description,
                status=item['status'],
                author_id=author.author_id if author else None,
                category_id=category.category_id if category else None,
                publishedDate=published_date
            )

            if short_description is not None:
                book.shortDescription = short_description
            if long_description is not None:
                book.longDescription = long_description
            if thumbnail_Url is not None:
                book.thumbnailUrl = thumbnail_Url
            if is_bn is not None:
                book.isbn = is_bn

            if existing_book is None:
                session.add(book)

        session.commit()
        session.close()


def insert_data_to_authors(file_name):
    Session = sessionmaker(bind=engine)
    session = Session()
    file_path = file_name
    with open(file_path, 'r') as json_file:
        data = json.load(json_file)

    #authors_set = set()

    for item in data:
        authors = item.get('authors', [])
        for author_name in authors:
            existing_author = session.query(Authors).filter_by(author_name=author_name).first()

            if existing_author is None:
                author = Authors(author_name=author_name)
                session.add(author)

    session.commit()
    session.close()


def insert_data_to_categories(file_name):
    Session = sessionmaker(bind=engine)
    session = Session()
    file_path = file_name
    with open(file_path, 'r') as json_file:
        data = json.load(json_file)

    for item in data:
        categories = item.get('categories', [])
        for category_name in categories:
            existing_category = session.query(Categories).filter_by(category_name=category_name).first()

            if existing_category is None:
                category = Categories(category_name=category_name)
                session.add(category)

    session.commit()
    session.close()


# def insert_data_from_json_authorsbooks(file_name):
#     Session = sessionmaker(bind=engine)
#     session = Session()
#     file_path = file_name







if __name__ == '__main__':
    with main.app_context():
        db.create_all()

        insert_data_to_authors('amazon.books.json')
        insert_data_to_categories('amazon.books.json')
        insert_data_from_json_books('amazon.books.json')

    main.run(debug=False)