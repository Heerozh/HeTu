#  """
#  @author: Heerozh (Zhang Jianhao)
#  @copyright: Copyright 2024, Heerozh. All rights reserved.
#  @license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
#  @email: heeroz@gmail.com
#  """

import numpy as np

from hetu.data import BaseComponent, Permission, define_component, property_field
from hetu.sourcegen.csharp import generate_component


def test_component_csharp_gen():
    @define_component(namespace="HeTu", volatile=True, permission=Permission.ADMIN)
    class TestComponent(BaseComponent):
        int64b: ">i8" = property_field(0)
        int64l: "<i8" = property_field(0)
        float64: np.float64 = property_field(0.0)
        str7: "U7" = property_field("")
        int16: np.int16 = property_field(0)
        uint8: np.uint8 = property_field(0)
        uint64: np.uint64 = property_field(0)
        float16: np.float16 = property_field(0.0)
        bool_: bool = property_field(False)

    code = "\n".join(generate_component(TestComponent))

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
    assert code == expect
