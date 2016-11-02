from sqlalchemy.inspection import inspect
import datetime
import json
import logging
logger = logging.getLogger(__name__)


def get_class_by_tablename(name, registry):
    for c in registry.values():
        if c.__tablename__ == name:
            return c


def compute_key(d):
    d = d.copy()
    for k, v in d.items():
        if isinstance(v, (datetime.date, datetime.datetime)):
            d[k] = v.isoformat()
    return json.dumps(d)


def upsert(session, model, cache, **kwargs):
    key = (model, compute_key(kwargs))
    obj = session.query(model).filter_by(**kwargs).one_or_none()
    if obj is None:
        obj = cache.get(key)
        if obj is None:
            obj = model(**kwargs)
            session.add(obj)
            cache[key] = obj
            logger.debug('created {}'.format(obj))
        else:
            logger.debug('fetched {} from cache'.format(obj))
    else:
        logger.debug('fetched {} from DB'.format(obj))
    return obj


def attributes(model):
    return [x.key for x in inspect(model).attrs
            if not hasattr(x, 'target')
            and not x.key.endswith('id')]  # XXX: handle 'id' better (PK, ...)


def relations(model, registry):
    return [(k, get_class_by_tablename(v.target.name, registry)) 
            for k, v in inspect(model).relationships.items()]


def build(session, models, data):
    with session.no_autoflush:
        cache = {}
        registry = dict([(x.__mapper__.class_.__name__, x) for x in models])
        root = list(data.keys())[0]
        model = registry[root]
        return build_tree(session, registry, data[root], model, cache)


def build_tree(session, registry, data, model, cache):

    if isinstance(data, (list, tuple)):
        return [build_tree(session, registry, d, model, cache) for d in data]

    attrs = attributes(model)
    rels = relations(model, registry)

    kwargs = {}
    for a in attrs:
        if a in data:
            kwargs[a] = data[a]

    obj = upsert(session, model, cache, **kwargs)

    for r, k in rels:
        if r in data:
            sub = build_tree(session, registry, data[r], k, cache)
            setattr(obj, r, sub)

    return obj
