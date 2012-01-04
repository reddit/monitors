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
