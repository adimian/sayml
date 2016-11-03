import flask
import flask_sqlalchemy
import pytest
import os.path as osp
from sayml import build

here = osp.dirname(__file__)


@pytest.fixture
def db():
    app = flask.Flask('test')
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    app.config['SQLALCHEMY_ECHO'] = False
    db = flask_sqlalchemy.SQLAlchemy(app)

    class Product(db.Model):
        __tablename__ = 'product'
        id = db.Column(db.Integer, primary_key=True)
        name = db.Column(db.String(50), unique=True, nullable=False)
        color = db.Column(db.String(30))

    class Ticket(db.Model):
        __tablename__ = 'ticket'

        id = db.Column(db.Integer, primary_key=True)
        date = db.Column(db.Date)
        customer_id = db.Column(db.Integer,
                                db.ForeignKey('customer.id'),
                                nullable=False)
        customer = db.relationship('Customer')

    class TicketLine(db.Model):
        __tablename__ = 'ticket_line'
        __table_args__ = (db.UniqueConstraint('ticket_id', 'product_id'),)

        id = db.Column(db.Integer, primary_key=True)
        ticket_id = db.Column(db.Integer,
                              db.ForeignKey('ticket.id'),
                              nullable=False)
        ticket = db.relationship('Ticket', backref=db.backref('lines'))

        product_id = db.Column(db.Integer,
                               db.ForeignKey('product.id'),
                               nullable=False)
        product = db.relationship('Product')

        quantity = db.Column(db.Integer, default=1, nullable=False)

    class Customer(db.Model):
        __tablename__ = 'customer'
        id = db.Column(db.Integer, primary_key=True)
        name = db.Column(db.String(50), unique=True, nullable=False)
        purchases = db.relationship('Ticket')

    return {'db': db, 'models': [Product, Ticket, TicketLine, Customer]}


@pytest.fixture
def data():
    from yaml import load
    return load(open(osp.join(here, 'data', 'create.yml')))


@pytest.fixture
def data_two():
    from yaml import load
    return load(open(osp.join(here, 'data', 'create_two.yml')))


def test_create(db, data):
    db['db'].create_all()
    session = db['db'].session
    build(session, db['models'], data)
    session.commit()

    model = db['models'][1]
    ticket = session.query(model).one()
    assert ticket.customer.name == 'Mr Customer'
    assert ticket.lines[-1].quantity == 5
    assert ticket.lines[-1].product.name == 'Cereal Box'


def test_create_two(db, data_two):
    db['db'].create_all()
    session = db['db'].session
    build(session, db['models'], data_two)
    session.commit()

    Product, Ticket, TicketLine, Customer = db['models']
    assert session.query(Ticket).count() == 2
    assert session.query(Product).count() == 2

    build(session, db['models'], data_two)
    session.commit()
    assert session.query(Product).count() == 2