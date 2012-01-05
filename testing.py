import tempfile

import alerts

def stub(namespace, name):
    '''Allows a function to override a value and restore it upon returning.'''
    def decorator(f):
        def wrapped(*args, **kwargs):
            try:
                orig_value = getattr(namespace, name)
                has_orig_value = True
            except AttributeError:
                has_orig_value = False
            try:
                return f(*args, **kwargs)
            finally:
                if has_orig_value:
                    setattr(namespace, name, orig_value)
                else:
                    try:
                        delattr(namespace, name)
                    except AttributeError:
                        pass
        return wrapped
    return decorator

def init_alerts(**sections):
    config = dict(
        harold=dict(host='localhost', port=8888, secret='secret'),
    )
    config.update(sections)
    with tempfile.NamedTemporaryFile() as f:
        for section, data in config.iteritems():
            f.write('[%s]\n' % section)
            for name, value in data.iteritems():
                f.write('%s = %s\n' % (name, value))
            f.write('\n')
        f.flush()
        alerts.init(config_path=f.name)
