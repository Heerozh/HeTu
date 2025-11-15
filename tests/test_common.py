
import numpy as np


def test_multimap():
    from hetu.common.multimap import MultiMap
    primary = [2, 4, 1, 3, 3, 4, 2, 3]
    second_ = [5, 3, 2, 1, 9, 8, 7, 6]
    d = MultiMap()
    for i, _ in enumerate(primary):
        d.add(primary[i], second_[i])

    # 测试查询
    np.testing.assert_array_equal(
        d.iloc(d.index(2)),
        np.array([5, 7]))
    np.testing.assert_array_equal(
        d.iloc(d.index(1)),
        np.array([2]))
    sel = d.index(2)
    np.testing.assert_array_equal(
        d.iloc(slice(sel.stop - 1, sel.start - 1, -1)),
        np.array([7, 5]))
    assert d.count() == 8
    assert d.count(0, 3) == 6
    assert d.count(5) == 0
    assert d.count(0) == 0
    np.testing.assert_array_equal(
        d.query(0),
        []
    )
    np.testing.assert_array_equal(
        d.query(1),
        [2]
    )
    np.testing.assert_array_equal(
        d.query(0, 3),
        [2, 5, 7, 1, 6, 9]
    )
    np.testing.assert_array_equal(
        d.query(3, 99),
        [1, 6, 9, 3, 8]
    )
    np.testing.assert_array_equal(
        d.query(99),
        []
    )

    # 测试添加
    d.add(2, 3)
    d.add(2, 9)
    np.testing.assert_array_equal(
        d.iloc(d.index(2)),
        np.array([3, 5, 7, 9]))

    # 测试删除
    d.remove(2, 5)
    np.testing.assert_array_equal(
        d.iloc(d.index(2)),
        np.array([3, 7, 9]))

    # 测试再次添加
    d.add(2, 5)
    np.testing.assert_array_equal(
        d.iloc(d.index(2)),
        np.array([3, 5, 7, 9]))

    # 测试pop
    np.testing.assert_array_equal(
        d.pop(2, 3),
        [3,5,7,9,1,6,9]
    )
    np.testing.assert_array_equal(
        d.query(2,3),
        []
    )

