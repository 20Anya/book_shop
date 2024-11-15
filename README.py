import sqlalchemy
import sqlalchemy as sq
from sqlalchemy.orm import declarative_base, relationship, sessionmaker
import json

user = ''  #имя пользователя
password = ''  #пароль пользователя
host = ''  #адрес хоста
port = 5432  #номер порта
name_db = ''  #название базы данных

DSN = f'postgresql://{user}:{password}@{host}:{port}/{name_db}'

Base = declarative_base()

class Publisher(Base):
    __tablename__ = "publisher"

    id = sq.Column(sq.Integer, primary_key=True)
    name = sq.Column(sq.String(length=40), nullable=False)


class Book(Base):
    __tablename__ = "book"

    id = sq.Column(sq.Integer, primary_key=True)
    title = sq.Column(sq.Text, nullable=False)
    publisher_id = sq.Column(sq.Integer, sq.ForeignKey("publisher.id"), nullable=False)

    publisher = relationship(Publisher, backref="books")

class Shop(Base):
    __tablename__ = "shop"

    id = sq.Column(sq.Integer, primary_key=True)
    name = sq.Column(sq.String(length=40), nullable=False)

    def __str__(self):
        return f'Магазин: {self.id} : {self.name}'

class Stock(Base):
    __tablename__ = "stock"

    id = sq.Column(sq.Integer, primary_key=True)
    book_id = sq.Column(sq.Integer, sq.ForeignKey("book.id"), nullable=False)
    shop_id = sq.Column(sq.Integer, sq.ForeignKey("shop.id"), nullable=False)
    count = sq.Column(sq.Integer, nullable = False)

    book = relationship(Book, backref="stocks")
    shop = relationship(Shop, backref="stocks")

class Sale(Base):
    __tablename__ = "sale"

    id = sq.Column(sq.Integer, primary_key=True)
    price = sq.Column(sq.Float, nullable = False)
    date_sale = sq.Column(sq.Date, nullable = False)
    stock_id = sq.Column(sq.Integer, sq.ForeignKey("stock.id"), nullable=False)
    count = sq.Column(sq.Integer, nullable = False)

    stock = relationship(Stock, backref="sales")

def create_tables(engine):
    # Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)


engine = sqlalchemy.create_engine(DSN)
create_tables(engine)

with open('fixtures.json', encoding='utf-8') as f:
    file_content = json.loads(f.read())


Session = sessionmaker(bind=engine)
session = Session()

for line in file_content:
    if line['model'] == 'publisher':
        pub = Publisher(id=line['pk'], name=line['fields']['name'])
        session.add(pub)
    if line['model'] == 'book':
        book = Book(id=line['pk'], title=line['fields']['title'], id_publisher=line['fields']['id_publisher'])
        session.add(book)
    if line['model'] == 'shop':
        shop = Shop(id=line['pk'], name=line['fields']['name'])
        session.add(shop)
    if line['model'] == 'stock':
        stock = Stock(id=line['pk'], id_shop=line['fields']['id_shop'], id_book=line['fields']['id_book'],
                      count=line['fields']['count'])
        session.add(stock)
    if line['model'] == 'sale':
        sale = Sale(id=line['pk'], price=line['fields']['price'], date_sale=line['fields']['date_sale'],
                    count=line['fields']['count'], id_stock=line['fields']['id_stock'])
        session.add(sale)
    else:
        pass
    session.commit()


search = str(input('Введите 1 для поиска по id издателя. Введите 2 для поиска по имени издателя: '))

if search == '1':
    req = str(input('Введите id издателя: '))
    for q in session.query(Shop).join(Stock.shop).join(Stock.book).join(Book.publisher).filter(Publisher.id ==
                                                                                               req).all():
        print(q)

if search == '2':
    req = str(input('Введите имя издателя: '))
    for q in session.query(Shop).join(Stock.shop).join(Stock.book).join(Book.publisher).filter(Publisher.name ==
                                                                                               req).all():
        print(q)

else:
    print('Введены некорректные данные, либо издатель не существует')
