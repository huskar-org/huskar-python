# Huskar SDK

- [Documentation](https://github.com/huskar-org/huskar-python)

- [Toturial](https://github.com/huskar-org/huskar-python)

- [CHANGELOG](CHANGELOG.md)


## Install

``pip install -e .``

With extra dependencies of doc:

``pip install -e ".[doc]"``

With extra dependencies of tests:

``pip install -e ".[test]"``

Install all
``make develop``


## Tests

run

``python setup.py test``
or
``make test`

## 分支版本

0.x.y:

* x 代表是否有不兼容.
* y 为自动同一兼容性下, 自动升级的版本. 会自动在线上安装.

总共维护 3 个不兼容.

* 最新版本分支: master
* 0.13.x 版本分支: v0.13.x
* 0.14.x 版本分支: v0.14.x


## Kazoo

Kazoo 仅在使用 `BootstrapHuskar` 时用到, 这是供 Huskar API 自举用的接口。

对于其他使用方, 建议只使用 `HttpHuskar` , 这是唯一受支持的 Huskar 接入方式。

我们依赖的 Kazoo 版本是基于上游 2.0 版本 fork 的[一个分支](https://github.com/huskar-org/kazoo/tree/eleme-2.0)，
其中 backport 了一些 hotfix 和特性。

至于为什么不升级到最新版本的 Kazoo，历史中留存了一份解释。

> 我们上次 `add_auth` 导致的 xid mismatch 的问题, 结合
> https://github.com/python-zk/kazoo/pull/305, 以及我对比 ZooKeeper Java Client
> 和 Kazoo 实现的结果，可以得出一个结论: Kazoo 的连接管理中 `def _invoke`
> 这种不经过队列直接递交连接时请求的行为是完全有可能导致 race condition 的…
> 目前的 Kazoo 版本结合 huskar-sdk 实现算是很小心地躲开已知的石子，
> 如果换个姿势就未必还能躲开了。
>
> 这个问题如果没有（通过 Kazoo 重构）彻底解决，我建议我们不要再抱有升级 Kazoo
> 的想法了。如果需要上游的某个 patch，我们可以 backport 到我们 fork 的分支。

## FAQ

### 在面板上添加的数据用 SDK 取不到
请确认：

1. Huskar SDK 连接的服务器和面板是否相同
2. Huskar SDK 使用的服务名称和面板的服务名称是否相同
3. 功能模块是否有弄错？配置和开关分别对应面板中的 `Config` 和 `Switch`
4. Huskar SDK 使用的版本是否 **过旧** ，一般来说我们推荐使用 [已发布的最新版](https://github.com/huskar-org/huskar-python/releases)


## 特性

* 开关修改实时生效
* 以/team/project/project_module/cluster/api为粒度进行管理，例如:/arch/test/test_233/test_6666/query_by_geohash
* 以user和team为单位进行权限分配，以api为粒度进行权限设置(change/delete/read)。
* 以比例形式设置接口开关，例如：设置开关大小为50%，则所有流量的50%经过接口处理，50%的流量返回默认。
* 默认返回的方式: 以decorator的方式设置接口返回的默认值.
