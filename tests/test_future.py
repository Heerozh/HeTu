


def test_duplicate_bug(mod_auto_backend, new_clusters_env):
    """测试未来调用常用的duplicated的system，component是否会按namespace隔离"""
    from hetu.system import define_system, Context
    from hetu.data.component import Permission
    # 定义2个不同的namespace的future call
    @define_system(
        namespace="ns1",
        permission=Permission.EVERYBODY,
        bases=('create_future_call:copy1',),
    )
    async def use_future_namespace1(ctx: Context, value, recurring):
        return await ctx['create_future_call:copy1'](
            ctx, -1, 'any_other_system', value, timeout=10, recurring=recurring)

    @define_system(
        namespace="ns2",
        permission=Permission.EVERYBODY,
        bases=('create_future_call:copy1',),
    )
    async def use_future_namespace2(ctx: Context, value, recurring):
        return await ctx['create_future_call:copy1'](
            ctx, -1, 'any_other_system', value, timeout=10, recurring=recurring)

    from hetu.system import SystemClusters
    SystemClusters().build_clusters('ns1')

    # 检查FutureCalls是否正确隔离
    from hetu.system.future import FutureCalls
    future_ns1 = list(FutureCalls.get_duplicates('ns1').values())
    future_ns2 = list(FutureCalls.get_duplicates('ns2').values())
    assert len(future_ns1) == len(future_ns2) == 1
    # Component的namespace并不会变
    assert future_ns1[0].namespace_ ==future_ns2[0].namespace_ == 'HeTu'
    assert future_ns1[0].component_name_ == future_ns2[0].component_name_

    # 检查component table manager是否正确隔离
    backend_component_table, get_or_create_backend = mod_auto_backend
    backend = get_or_create_backend()
    backends = {'default': backend}
    comp_tbl_classes = {'default': backend_component_table}

    from hetu import ComponentTableManager
    comp_mgr = ComponentTableManager(
        'ns1', 'server1', backends, comp_tbl_classes)

    assert comp_mgr.get_table(future_ns1[0]) is not None
    assert comp_mgr.get_table(future_ns2[0]) is None

