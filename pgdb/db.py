from sqlalchemy import create_engine, Column, Integer, String, DateTime, Float, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from pandas import DataFrame
Base = declarative_base()


class Article(Base):
    __tablename__ = 'articles_btc'

    ID = Column(Integer, primary_key=True)
    GUID = Column(String)
    PUBLISHED_ON = Column(DateTime)
    IMAGE_URL = Column(String)
    TITLE = Column(String)
    URL = Column(String)
    SOURCE_ID = Column(String)
    BODY = Column(String)
    KEYWORDS = Column(String)
    # Assuming 2 characters for language codes e.g., 'en', 'es'
    LANG = Column(String)
    UPVOTES = Column(Integer, default=0)
    DOWNVOTES = Column(Integer, default=0)
    SCORE = Column(Float)
    SENTIMENT = Column(String)
    STATUS = Column(String)
    SOURCE_DATA = Column(JSON)  # Assuming it's a JSON type field
    CATEGORY_DATA = Column(JSON)  # Assuming it's a JSON type field


# Connect to the PostgreSQL database
DATABASE_URL = "postgresql+psycopg2://postgres:testpasswd@localhost:5432/test-database"
engine = create_engine(DATABASE_URL)

# Create the table
Base.metadata.create_all(engine)

# Start a session to insert or query data
SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()


def save_article(article: Article):
    try:
        session.add(article)
        session.commit()
    except Exception as e:
        session.rollback()
        print(f"Error occurred while saving an article: {e}")


def save_articles(articles: list):
    try:
        session.add_all(articles)
        session.commit()
    except Exception as e:
        session.rollback()
        print(f"Error occurred while saving articles: {e}")


def article_list_from_df(dataframe: DataFrame):
    articles = []
    for _, row in dataframe.iterrows():
        article = Article(
            ID=row['ID'],
            GUID=row['GUID'],
            PUBLISHED_ON=row['PUBLISHED_ON_DATE'],
            IMAGE_URL=row['IMAGE_URL'],
            TITLE=row['TITLE'],
            URL=row['URL'],
            SOURCE_ID=row['SOURCE_ID'],
            BODY=row['BODY'],
            KEYWORDS=row['KEYWORDS'],
            LANG=row['LANG'],
            UPVOTES=row['UPVOTES'],
            DOWNVOTES=row['DOWNVOTES'],
            SCORE=row['SCORE'],
            SENTIMENT=row['SENTIMENT'],
            STATUS=row['STATUS'],
            SOURCE_DATA=row['SOURCE_DATA'],
            CATEGORY_DATA=row['CATEGORY_DATA']
        )
        articles.append(article)
    return articles


def save_articles_df(dataframe: DataFrame):
    articles = article_list_from_df(dataframe)
    save_articles(articles)


def get_oldest_article_published_on():
    try:
        oldest_article = session.query(Article).order_by(
            Article.PUBLISHED_ON).first()
        return oldest_article.PUBLISHED_ON
    except:
        return 0
