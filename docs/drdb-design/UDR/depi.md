## 核心组件DEPI
### 1. 简要需求说明
该组件的核心功能是为外部(如D-PLSQL引擎)提供相应语言的编程接口.待开发要点:
1. D-PLSQL模块从postgres中分离改造而来, 原本它通过SPI接口调用PG执行器, 现在我们要让它改用DEPI接口调用ZNBase执行器.
所以DEPI的初步需求是仿照postgres的SPI接口函数(SPI_exec, SPI_prepare等)实现一套接口,支持执行SQL语句,操作游标等功能, 满足对D-PLSQL全部需求的支持.

2. 将上述接口封装并暴露给用户,支持多种语言.第一步是支持Golang, 用户只需import depi包即可编写udr调用上述接口.

3. 支持用户以插件的形式编写并编译UDR, 服务端可以动态加载用户编译出的插件并调用UDR.

[PG11 SPI接口文档](www.postgres.cn/docs/11/spi.html)

### 2. 相关功能和代码描述
#### ZNBase的内部执行器
拟通过内部执行器来实现大部分DEPI接口功能.调用内部执行器执行SQL语句的示意代码:

    func (n *createTableNode) startExec(params runParams) error {
	    var RP RunParam
	    RP.Ctx = params.ctx
	    RP.ExeCfg = params.p.ExecCfg()
	    RP.EvalCtx = params.p.ExtendedEvalContext()
	    rows, cols, err := RP.ExeCfg.InternalExecutor.QueryWithCols(RP.Ctx, "query Internal", RP.EvalCtx.Txn, "select * from test")
	    //create table 
    }
    
由于本分支暂未合并语法部分, 目前借用createTable的startExec阶段进行测试验证.

使用RunParam结构体储存和传递调用内部执行器所需的运行参数

    type RunParam struct {
	    Ctx     context.Context
	    ExeCfg  *ExecutorConfig
	    EvalCtx *extendedEvalContext
    }
    
内部执行器和RunParam相关代码位于sql包下internal.go和internal_executor.go中

#### DEPI包

为解决循环依赖和封装问题, 引入RunParamInterface接口

    type RunParamInterface interface {
    	ExecStmt(stmt string) (int, error)
    	ExecReturnRows(stmt string) ([]RowDatums, error)
    	PrePare(name string, stmt string, argsType ...string) (string, error)
    	ExecPrepareWithName(name string, argsVal ...string) error
    }

Runparam结构体具体实现了该接口, 用户仅需import depi包并获取到Runparam即可调用接口方法. 

方法的具体实现主要依赖内部执行器, 后续的主要工作就是参照pg的SPI向RunParamInterface中添加方法, 最终和上层的D-PLSQL兼容.

这块面临的一些问题:

#####1. 关于SPI_prepare: 

在用户session中调用prepare再调用execute可以正常执行.


    prepare p1(int) as insert into test values($1);
    execute p1(2);

但在同一处用内部执行器连续执行这两句,在execute时报找不到p1.认为prepare是session级或更低, 拟使用SessionBoundInternalExecutor或调用更底层.

#####2. 关于数据类型:

执行查询语句时,内部执行器的返回值由tree.Datum接口类型及其多维数组构成.为了不向外部暴露tree包, depi包需要有一套自己的简单数据类型系统用来给go语言插件用户操作,并用统一的接口接收内部执行器的返回值.

定义depi.Val接口并实现了简单数据类型.后续应参照tree.Datum将常用类型添加进去,并为接口添加必要的方法.

#####3. Plugin插件

Plugin包是go官方提供的动态库加载方法, 我们用来实现插件化.

编写插件:

在main包(只能是main包不然会报错)中建立plugin.go文件,import depi包, 然后编写函数,在其中调用depi.RunParamInterface接口的方法实现自定义的功能.函数的参数必须包含一个depi.RunParamInterface.

例如:

    package main
    
    import "github.com/znbasedb/znbase/pkg/depi"
    
    func UDR001(RP depi.RunParamInterface, argv ...interface{}) error {
    	_, err := RP.ExecStmt("insert into defaultdb." + argv[0].(string) + " values(" + argv[1].(string) + ")")
    	return err
    }
用命令go build -buildmode=plugin ./pkg/depimain/plugin.go 编译插件,生成plugin.so动态库.

使用插件:

目前没引入有UDR语法, 暂时在createTable的startExec处测试.即建表语句就伴随着调用插件.例如:

    func (n *createTableNode) startExec(params runParams) error {
    
    	var RP RunParam
    	RP.Ctx = params.ctx
    	RP.ExeCfg = params.p.ExecCfg()
    	RP.EvalCtx = params.p.ExtendedEvalContext()
    
    	pl, perr := plugin.Open("/home/gocode/src/github.com/znbasedb/znbase/plugin.so")
    	if perr == nil {
    		UDRFuncInterface, perr := pl.Lookup("UDR001")
            	if perr == nil {
            		if UDRFunc, ok := UDRFuncInterface.(func(depi.RunParamInterface, ...interface{}) error); ok {
            		    perr := UDRFunc(RP, "test", "2")
            		    println(perr)
            		}
            	}
            		
    	}
    }
    
未解决的插件问题:将go1.12.7源码中/home/go/src/runtime/plugin.go 51:56行注释掉后才能运行,否则运行时报错plugin was built with a different version of package, 原因暂时不明.