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
    db = flask_sqlalchemy.SQLAlchemy(app)

    class Product(db.Model):
        __tablename__ = 'product'
        id = db.Column(db.Integer, primary_key=True)
        name = db.Column(db.String(50), unique=True, nullable=False)

    class Ticket(db.Model):
        __tablename__ = 'ticket'

        id = db.Column(db.Integer, primary_key=True)
        customer_id = db.Column(db.Integer,
                                db.ForeignKey('customer.id'),
                                nullable=False)
        customer = db.relationship('Customer')
        lines = db.relationship('TicketLine')

    class TicketLine(db.Model):
        __tablename__ = 'ticket_line'
        __table_args__ = (db.UniqueConstraint('ticket_id', 'product_id'),)

        id = db.Column(db.Integer, primary_key=True)
        ticket_id = db.Column(db.Integer,
                              db.ForeignKey('ticket.id'),
                              nullable=False)
        ticket = db.relationship('Ticket')

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


def test_create(db, data):
    db['db'].create_all()
    session = db['db'].session
    ticket = build(session, db['models'], data)
    session.commit()

    assert ticket.customer.name == 'Mr Customer'
