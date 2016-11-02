from sqlalchemy.inspection import inspect


def get_class_by_tablename(name, registry):
    for c in registry.values():
        if c.__tablename__ == name:
            return c


def upsert(session, model, **kwargs):
    obj = session.query(model).filter_by(**kwargs).one_or_none()
    if obj is None:
        obj = model(**kwargs)
        session.add(obj)
    return obj


def attributes(model):
    return [x.key for x in inspect(model).attrs
            if not hasattr(x, 'target')
            and not x.key.endswith('id')]  # XXX: handle 'id' better (PK, ...)


def relations(model, registry):
    return [(k, get_class_by_tablename(v.target.name, registry)) 
            for k, v in inspect(model).relationships.items()]


def build(session, models, data):
    registry = dict([(x.__mapper__.class_.__name__, x) for x in models])
    root = list(data.keys())[0]
    model = registry[root]
    return build_tree(session, registry, data[root], model)


def build_tree(session, registry, data, model):

    if isinstance(data, (list, tuple)):
        return [build_tree(session, registry, d, model) for d in data]

    attrs = attributes(model)
    rels = relations(model, registry)

    kwargs = {}
    for a in attrs:
        if a in data:
            kwargs[a] = data[a]

    obj = model(**kwargs)

    for r, k in rels:
        if r in data:
            sub = build_tree(session, registry, data[r], k)
            setattr(obj, r, sub)

    return obj
