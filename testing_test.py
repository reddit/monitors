import unittest

import testing

class StubTest(unittest.TestCase):
    def setUp(self):
        class Object:
            x = 'x'
        self.object = Object()

    def test_return(self):
        @testing.stub(self.object, 'x')
        def test():
            self.assertEquals('x', self.object.x)
            self.object.x = 'y'
            return self.object.x
        self.assertEquals('y', test())
        self.assertEquals('x', self.object.x)

    def test_raise(self):
        @testing.stub(self.object, 'x')
        def test():
            self.object.x = 'y'
            self.assertEquals('y', self.object.x)
            raise RuntimeError
        self.assertRaises(RuntimeError, test)
        self.assertEquals('x', self.object.x)

    def test_undefined(self):
        @testing.stub(self.object, 'undef1')
        @testing.stub(self.object, 'undef2')
        def test():
            self.assertRaises(AttributeError, lambda: self.object.undef1)
            self.object.undef1 = 1
            self.assertEquals(1, self.object.undef1)
            self.assertRaises(AttributeError, lambda: self.object.undef2)
        test()
        self.assertRaises(AttributeError, lambda: self.object.undef1)
        self.assertRaises(AttributeError, lambda: self.object.undef2)

if __name__ == '__main__':
    unittest.main()
