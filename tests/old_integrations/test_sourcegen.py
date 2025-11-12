import unittest
import numpy as np
from hetu.sourcegen.csharp import dtype_to_csharp, generate_component
from hetu.data import BaseComponent, Property, define_component, Permission


class MyTestCase(unittest.TestCase):

    def test_component_csharp_gen(self):
        @define_component(namespace='HeTu', persist=False, permission=Permission.ADMIN)
        class TestComponent(BaseComponent):
            int64b: '>i8' = Property(0)
            int64l: '<i8' = Property(0)
            float64: np.float64 = Property(0.0)
            str7: 'U7' = Property('')
            int16: np.int16 = Property(0)
            uint8: np.uint8 = Property(0)
            uint64: np.uint64 = Property(0)
            float16: np.float16 = Property(0.0)
            bool_: bool = Property(False)

        code = '\n'.join(generate_component(TestComponent))

        expect = """
class TestComponent: IBaseComponent
{
    public long id { get; set; }
    public sbyte bool_;
    public float float16;
    public double float64;
    public short int16;
    public long int64b;
    public long int64l;
    public string str7;
    public ulong uint64;
    public byte uint8;
}
"""
        self.assertEqual(code, expect)


if __name__ == '__main__':
    unittest.main()
