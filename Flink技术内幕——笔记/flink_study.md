graph

算子  <!--调用 -->→ Transform()   <!-- 新建 OneInputTransformation-->
→ getExecutionEnvironment.addOperator(transformation)  <!-- 保存Transformation到SteamExecutionEnvironment  -->
→ 生成 StreamGraph <!--创建setChaining()   "算子链"OperatorChain-(HashCode) -->
→ JobGraph 
→ ExecutionGraph
<!-- JsonArchivist接口 - 基于ExecutionGraph生成JSON，供Handler(处理器)实现-查询作业执行图信息 -->

[[深入解析 Flink 的算子链机制 - 云+社区 - 腾讯云 (tencent.com)](https://cloud.tencent.com/developer/article/1751614)](https://cloud.tencent.com/developer/article/1751614)

[(70条消息) Flink-作业提交流程_迷路剑客的博客-CSDN博客_flink作业提交流程](https://blog.csdn.net/baichoufei90/article/details/108274922)



# 算子

## Lambda表达式：使用到泛型时，需显示声明类型信息

```java
DataStream<String> stream1 = clicks.map(event -> event.url);
```

由于 OUT 是 String 类型而不是泛型，所以 Flink 可以从函数签名 OUT map(IN value)  的实现中自动提取出结果的类型信息

```java
DataStream<String> stream2 = clicks.flatMap((event, out) -> {
out.collect(event.url);
```

void flatMap(IN value, Collector  out) 被 Java 编译器编译成了 void flatMap(IN value, Collector out)，也就是说将 Collector 的泛 型信息擦除掉了,无法推断输出的类型

需显示指定类型

```java
// flatMap 使用 Lambda 表达式，必须通过 returns 明确声明返回类型
DataStream<String> stream2 = clicks.flatMap((Event event, Collector<String> 
out) -> {
out.collect(event.url);
}).returns(Types.STRING);

```

## Rich Function Classes

例如：RichMapFunction、RichFilterFunction、 RichReduceFunction 等

Rich Function 有生命周期的概念：

* open() ：

是 Rich Function 的初始化方法，也就是会开启一个算子的生命周期。当 一个算子的实际工作方法例如 map()或者 filter()方法被调用之前，open()会首先被调 用。所以像文件 IO 的创建，数据库连接的创建，配置文件的读取等等这样一次性的 工作，都适合在 open()方法中完成。

* close()方法

是生命周期中的最后一个调用的方法，类似于解构方法。一般用来做一 些清理工作。

```
需要注意的是，这里的生命周期方法，对于一个并行子任务来说只会调用一次；而对应的，
实际工作方法，例如 RichMapFunction 中的 map()，在每条数据到来后都会触发一次调用。
```

# WaterMark

* assignTimestampsAndWatermarks()

主要用来为流中的数据分配时间戳，并生成水位线 来指示事件时间

```
assignTimestampsAndWatermarks()方法需要传入一个 WatermarkStrategy 作为参数，这就
是 所 谓 的 “ 水 位 线 生 成 策 略 ” 。 WatermarkStrategy 中 包 含 了 一 个 “ 时 间 戳 分 配
器”TimestampAssigner 和一个“水位线生成器”WatermarkGenerator。
```

WatermarkGenerator：主要负责按照既定的方式，基于时间戳生成水位线。在 WatermarkGenerator 接口中，主要又有两个方法：onEvent()和 onPeriodicEmit()。

* onEvent：每个事件（数据）到来都会调用的方法，它的参数有当前事件、时间戳， 以及允许发出水位线的一个 WatermarkOutput，可以基于事件做各种操作
* onPeriodicEmit：周期性调用的方法，可以由 WatermarkOutput 发出水位线。周期时间 为处理时间，可以调用环境配置的.setAutoWatermarkInterval()方法来设置，默认为 200ms。



### 内置水位线生成器 WatermarkGenerator

1. 有序流
2. 乱序流
3. 自定义水位线策略

### 水位线传递

![image-20220525200242644](https://raw.githubusercontent.com/xylong-xd/Flink_study/main/Flink%E6%8A%80%E6%9C%AF%E5%86%85%E5%B9%95%E2%80%94%E2%80%94%E7%AC%94%E8%AE%B0/img/image-20220525200242644.png)

# 窗口 Window

![image-20220526093410801](img\image-20220526093410801.png)

## 实现类
* EvictingWindowOperator(带有驱逐器 <!-- 过滤 -->)      
* WindowOperation


## 实现流程(EvictingWindowOperator)

   WindowAssigner.assignWindows()  <!-- 分配窗口  -->
→if MergingWindowAssigner ? getMergingWindowSet().addWindow()
→ setCurrentNamespace()  <!-- 设置命名空间 -->
→ if triggerResult.isFire()   <!-- 触发器: 调用驱逐器 -->
→ registerCleanupTimer(window) <!-- 注册窗口本身的清除定时器 -->

##  窗口分配器 (assignWindows)
###  滚动窗口

TumblingEventTimeWindows    <!-- 基于事件时间的窗口 -->
TumblingProcessingTimeWindows   <!-- 基于系统时间的窗口 -->

###  滑动窗口

SlidingEventTimeWindows
SlidingProcessingTImeWIndows

###  会话窗口

EventTimeSessionWindows
ProcessingTimeSessionWindows
DynamicEventTimeSessionWindows     <!-- 不断变化 -->  →  <!-- 会话窗口不断合并 -->
DynamicProcessingTimeSessionWindows


####  窗口的合并     

1. 找出合并之前的窗口集合和合并之后的窗口

2. 找出合并之后的窗口对应的状态窗口(从合并窗口集合中选第一个窗口的状态窗口)

3. 执行merge  --> MergingWindowSet.addwindow()

   ```
   MergingWindowSet中有一个map,(Mao<Window,Window>),保存窗口和状态窗口对应关系，通过（1,8）对应的状态窗口（1,4）获取合并后的状态，即数据集
   ```

   ![merge](C:\Users\龙星宇\Desktop\xue\flink_study\img\merge.png)

   



###  全局窗口
GlobalWindow


### 触发器

返回值： 
                    CONTINUE    <!--  什么也不做 -->
                    FIRE                     <!-- 出发窗口计算 -->
                    PURGE               <!-- 清楚窗口中的数据 -->
                    FIRE_AND_PURGE   <!-- 触发计算并清除数据 -->
                    
常用触发器：
                     EventTimeTrigger    <!-- 水位线大于窗口的结束时间触发 -->
                     ProcessingTimeTregger  <!-- 系统时间大于窗口的结束时间 -->
                     CountTrigger     <!-- 数据量大于一定值 -->
                     DeltaTrigger     <!-- 根据阈值函数计算出的阈值判断窗口是否触发 -->


实现:

   *InternalTimerService 为flink内部定时器的存储管理类
   InternalTimerServiceImlp 内部维护了一个有序队列*
            
   InternalTimerServiceImlp.registerEventTimeTimer()   InternalTimerServiceImlp.onProcessingTime()  <!-- 注册定时器 --> 
   将定时器放到一个有序队列(eventTimeTimersQueue),等水位线来触发


###  窗口函数

*窗口触发后的计算过程*


* ReduceFunction  –  <!-- 增量计算，每条数据都会触发计算，窗口状态中只保留计算结果 -->
* AggregateFunction –  <!-- 增量计算，每条数据都会触发计算，窗口状态中只保留计算结果 -->
* ProcessWindowFuntion – <!-- 保留所有数据，效率低 -->


#### ReduceFunction(输入，输出)
包装类：
                        WindowFunction  – 指导具体的窗口函数怎么计算                      
                         InternalWindowFunction – 封装窗口数据的类型，然后实际调用WindowFunction
                         
                         
传入WindowedStream → 放到StateDescriptor → 生成ReducingState                         
                        
                        
                        
#### AggregateFunction(输出，计算，输入)
*为ReduceFunction的扩展*


#  运行时组件与通讯
##  运行时组件
* REST
* Dispatcher
* JobMaster
* ResourceManager
* TaskExecutor

![778d472855948dfb04cff95e3282aea3.png](./img\778d472855948dfb04cff95e3282aea3.png)


###  REST

*为客户端和前端提供HTTP服务*

核心：
                WebMonitorEndpoint 类
       ![WebMonitorEndpoint.png](./img\WebMonitorEndpoint.png)
                
MiniDispatcherRestEndpoint – Per-Job模式
DispatcherRestEndpoint     – Session模式

WebMonitorEndpoint 启动（完成即可为外部提供REST服务）：

1. 初始化处理外部请求的 Handler
2. 将处理外部请求的 Handler注册到路由器（Router）
3. 创建并启动 NettyServer
4. 启动 Leader选举服务
   

1） 初始化所有 Handler
       DispatcherRestEndpoint.initializeHandlers()（继承于WebMonitorEndpoint实现的接口方法） (多添加JobSubmitHandler,开启Web提交功能），（添加WebSubmissionExtension类里对应的Handler,就是 *Flink UI* 中 *Submit New Job* 选项卡中的相关的请求)
       
       所有的Handler继承自 AbstractHandler
       AbstractHandler会继承 SimpleChannelInboundHandler 可以添加到ChannelPipeline, 来处理Channel入站的数据以及各种状态变换


2） Handler注册 Router


3） 创建与启动 NettyServer

两部分：
     * 初始化处理Channel
     * 绑定端口启动

初始化 Channel 会创建 ChannelPipeline

    ChannelPipeline.RouterHandler 负责将HTTP请求路由到正确的Handler（通过初始化Handler注册到Router的注册信息，找到HTTP请求对应的路由结果，如果为空，返回404；根据路由结果，触发对应Handler的消息处理）


​        
4） 启动 Leader选举服务



###  Dispatcher

    Dispatcher 组件负责接收作业的提交。对作业进行持久化、产生新的JonMaster执行作业、在JobManager节点崩溃恢复时恢复所有作业的执行，记忆管理作业对应JobMaster的状态

Dispatcher 组件相关类图：
![Dispatcher.png](./img\Dispatcher.png)


StandaloneDispatcher   – Session
MiniDispatcher         – Pre-Job

    REST将作业提交到 Dispatcher 是通过 RPC调用 Dispatcher 实现 DispatcherGateway 的submitJob 方法完成的

####  接收到REST提交作业的消息后的处理过程


1. 检查作业是否重复，防止一个作业在JobManager进程中被多次调度运行
2. 执行该作业前一次运行未完成的终止逻辑（同一个jobId的作业）
3. 持久化作业的jobGraph
4. 创建JobManagerRunner
5. JobManagerRunner构建JobMaster用来负责作业的运行
6. 启动JobManagerRunner



##### 检查作业是否重复

jobManagerRunnerFutures 属性在创建jobManagerRunner成功时会添加数据，失败及移除作业时会移除数据 → 判断作业是否正在执行中

RunningJobRegistry.JobSchedulingStatus.DONE  → 判断是否执行过

##### 作业提交过程

```
先执行作业的前一次未完成的退出逻辑，在执行持久化和运行作业
```

- 获取前一次未完成的终止的处理逻辑方法   <=  该作业：运行中？返回还在运行中的异常，否则从终止作业的进度列表中获取
- 持久化作业，SumittedJobGraphStore 对作业的JobGraph 信息进行持久化 =>  JobManager 崩溃恢复时 可以恢复作业
- 运行作业
  - 创建JobManagerRunner
  - 将创建JobManagerRunner的进度记录到已在运行的作业列表，表示该作业已在执行
  - 启动JobManagerRunner  

​     

### ResourceManager

```
ResourceManager 组件负责资源的分配与释放，以及资源状态的管理
```



ResourceManage类图：

![ResourceManager](./\img\ResourceManager.png)



#### ResourceManager 与其他组件的通信



1. REST 通过Dispatcher 透传或者直接与ResourceManager 通信来获取 TaskExecutor的详细信息，集群的资源情况，TaskExecutor Metric 查询服务的信息，TaskExecutor的日志与标志输出。<!-- 具体体现在 Flink UI -->
2. JobMaster 与 ResourceManager 的交互体现在申请Slot，释放Slot，将JobMaster注册到ResourceManager，以及组件间的心跳。
3. TaskExecutor   ——————   将TaskExecutor 注册到ResourceManager ，汇报TaskExecutor上Slot的情况，以及组件间的心跳。

![a48044f9c1c547f58ec4cf2a1ee16683](./img\a48044f9c1c547f58ec4cf2a1ee16683.png)



#### SlotManager

``` 
ResourceManager 通过SlotManager管理TaskManager 注册过来的Slot，供多个JobMaster的SlotPool来申请和分配
```

1. Slot 申请与分配

Slot  -> AllocationID  => 避免Slot重复提交

匹配空闲的Slot（TaskManagerSlot）

申请TaskManager资源/分配空闲的Slot  =>  TaskManagerSlot 的状态：空闲（FREE）-->  待分配（PENDING），绑定Slot申请请求，想TaskExecutor 请求异步占有对应Slot，返回结果：acknowledge 不为空 => PENDING --> ALLOCATED（已分配）



2. Slot 注册与分配

TaskManager 启动    -->    注册到ResourceManager   -->   Slot信息汇报给ResourceManager



#### TaskManagerSlot状态变换



### JobMaster



![JobMaster](C:\Users\龙星宇\Desktop\xue\flink_study\img\JobMaster.png)



* Scheduler :  负责 ExecutionGraph的调度（申请Slot）
* CheckpointCoordinator： 负责作业检查点的协调
* SlotPool： ExecutionGraph 调度时提供Slot 及对Slot进行生命周期管理（一个作业有一个SlotPool）



#### SlotPool申请Slot

1. 发起Slot请求

```
Scheduler 调度作业时需分配Slot，在SlotPool中没有匹配的空闲Slot时，会发起Slot请求，申请新的Slot（向ResourceManager）
```

申请过程：

​	创建分配ID（allocationId）

​	添加到待分配的请求列表（pendingRequests）

​	监控完成情况

2. 接收来自TaskExecutor 的Slot

```
信息交互通常要检测对方是否已经注册或上线
```

添加到SlotPool



### TaskExecutor



![TaskExecutor](C:\Users\龙星宇\Desktop\xue\flink_study\img\TaskExecutor.png)

```
TaskExecutor 是TaskManager 的核心部分，负责多个Task的执行
```

```
TaskManagerRunner 是 TaskManager 的执行入口，负责构建TaskExecutor 的网络、I/O管理、内存管理、RPC服务、HA服务及启动TaskExecutor
```

TaskExecutor 与：

​				ResourceManager 初次建立通信是在ResourceManager 申请和启动TaskExecutor

​							*TaskExecutor 启动后，通过HA服务监听到ResourceManager的Leader，主动发送消息建立连接*

​				JobMaster ， ResourceManager 向TaskExecutor申请Slot时， TaskExecutor会根据申请Slot中的作业信息，将Slot提供给JobMaster



```
Slot是划分TaskExecutor资源的基本逻辑单元
```



#### 接收来自ResourceManager的Slot请求

```
入口方法（RPC方法）为 requestSlot
```

1. 是否建立连接
2. 分配占有Slot( TaskSlotTable的allocateSlot 方法占有Slot) 
3. 提供给JobMaster（通过jobLeaderService监听对应的JobMaster的Leader信息建立连接）



#### 将Slot提供给JobMaster

建立连接

从TaskSlotTable 中筛选出 已被该作业占有但不处于活跃状态（ALllocated）的TaskSlot

提取信息组装给SlotOffer类别

发送 offerSlot请求 将Slot提供信息列表提供给JobMaster



### TaskExecutor 中的Slot状态如何与JobMaster、ResourceManager中的一致 

```
JobMaster向TaskExecutor定时发送心跳消息里的AllocatedSlopReport
TaskExecutor向ResourceManager定时发送心跳消息里的SlotReport
```





## 组件间通信

### Akka与Actor





## 运行时组件的高可用



*特指 Master 节点运行时的高可用*



```
通过ZooKeeper、HDFS共同实现
```



### Master节点上组件的高可用

```
单个Flink集群，默认只有一个Master节点，会造成单点问题
```

```
Master 的首领选举和检索是依赖Curator客户端与ZooKeeper服务交互实现的
```

#### Curator

选举通过Curator的LeaderLatch实现 ：

​	在ZooKeeper中选择一个根目录（如/leaderLatch ），多个进程发起选举时，会往根目录创建临时有序节点，编号最小的节点即Leader，没选上的节点监听Leader节点的删除事件，被删除后重新选举

Curator暴露于ZooKeeper的状态：

![未命名绘图](.\img\未命名绘图.png)



#### Leader选举



#### 运行时组件高可用存在的问题

* 对ZooKeeper的连接过于敏感



* Master节点异常会影响到作业



# 状态管理与容错

````
通过引入状态的概念对数据处理时的快照进行管理，同时使用检查点机制定时将任务状态上报与存储  —— 保证Exactly-once
````



## 状态

* Keyed State
* Operator State

各自分为 Raw State(原始状态)，Managed State(可管理状态  -> 官方推荐)



#### Managed Keyed State  ////     rocksdb

```
只可以使用在KeyedStream    -- ValueState,ListState,ReducingState,AggregatingState,FoldingState,MapState
如果要使用这些状态，需声明StateDescriptor
```

*所有 Managed Keyed StateDescriptor 的父类均为 StateDescriptor*

```java
public abstract class StateDescriptor<S extends State, T> implements Serializable
```

```java
public enum Type {
		/**
		 * @deprecated Enum for migrating from old checkpoints/savepoint versions.
		 */
		@Deprecated
		UNKNOWN,
		VALUE,
		LIST,
		REDUCING,
		FOLDING,
		AGGREGATING,
		MAP
	}
```



StateDescriptor提供了三个构造方法：

```java
	/**
	 * Create a new {@code StateDescriptor} with the given name and the given type serializer.
	 *
	 * @param name The name of the {@code StateDescriptor}.
	 * @param serializer The type serializer for the values in the state.
	 * @param defaultValue The default value that will be set when requesting state without setting
	 *                     a value before.
	 */
	protected StateDescriptor(String name, TypeSerializer<T> serializer, @Nullable T defaultValue) {
		this.name = checkNotNull(name, "name must not be null");
		this.serializerAtomicReference.set(checkNotNull(serializer, "serializer must not be null"));
		this.defaultValue = defaultValue;
	}

	/**
	 * Create a new {@code StateDescriptor} with the given name and the given type information.
	 *
	 * @param name The name of the {@code StateDescriptor}.
	 * @param typeInfo The type information for the values in the state.
	 * @param defaultValue The default value that will be set when requesting state without setting
	 *                     a value before.
	 */
	protected StateDescriptor(String name, TypeInformation<T> typeInfo, @Nullable T defaultValue) {
		this.name = checkNotNull(name, "name must not be null");
		this.typeInfo = checkNotNull(typeInfo, "type information must not be null");
		this.defaultValue = defaultValue;
	}

	/**
	 * Create a new {@code StateDescriptor} with the given name and the given type information.
	 *
	 * <p>If this constructor fails (because it is not possible to describe the type via a class),
	 * consider using the {@link #StateDescriptor(String, TypeInformation, Object)} constructor.
	 *
	 * @param name The name of the {@code StateDescriptor}.
	 * @param type The class of the type of values in the state.
	 * @param defaultValue The default value that will be set when requesting state without setting
	 *                     a value before.
	 */
	protected StateDescriptor(String name, Class<T> type, @Nullable T defaultValue) {
		this.name = checkNotNull(name, "name must not be null");
		checkNotNull(type, "type class must not be null");

		try {
			this.typeInfo = TypeExtractor.createTypeInfo(type);
		} catch (Exception e) {
			throw new RuntimeException(
					"Could not create the type information for '" + type.getName() + "'. " +
					"The most common reason is failure to infer the generic type information, due to Java's type erasure. " +
					"In that case, please pass a 'TypeHint' instead of a class to describe the type. " +
					"For example, to describe 'Tuple2<String, String>' as a generic type, use " +
					"'new PravegaDeserializationSchema<>(new TypeHint<Tuple2<String, String>>(){}, serializer);'", e);
		}

		this.defaultValue = defaultValue;
	}

```



* Managed Keyed State的声明是在函数类的open方法初始化的   —— 初始化完成就说明状态可用

* Flink通过RuntimeContext 操作状态 <!-- RuntimeContext提供不同的getState方法 ，如 ValueState-->

  [flink runtimeContext_哥伦布112的博客-CSDN博客](https://blog.csdn.net/u013939918/article/details/106641615)

getState：		

- [ ] 获取KeyedStateStore  <!-- 每一次状态变更都会同步到 KeyedStateStore -->
- [ ] 状态序列化方法初始化 <!-- 提供一个序列化方法，一个状态只会初始化一次 -->
- [ ] 从KeyedStateStore中得到状态的初始值 <!-- 任务第一次启动 ->  默认值； 从检查点启动 -> 获得从stateBackend中恢复的状态值 -->

* 状态的使用

调用状态的 value(), update() 方法   =>  循环调用实现了状态的读取与更新



* 实现

heap / rocksdb

```
MapState 有 HeapMapState 和 RocksDBMapState 两个实现类
```

都要重写getKeySerializer, getNamespaceSerializer, getValueSerializer ,对应key，namespace，value，的序列化器，并要根据Keyed State 的类型实现相应接口

```
ValueState 有 value(),update()，在HeapValueState,RocksDBValueState中会重写这两个方法，实现状态的读取和更新
```

heap: 

​	每种状态关联着一个StateTable ，对状态的更新读取通过 StateTable 进行。

rocksdb:

​	每种状态关联着一个ColumnFamilyHandle，(是rocksdb的内部类)，对应RocksDB列簇，rocksdb状态变更通过ColumnFamilyHandle实现。

[(70条消息) Flink RocksDB_一直奔跑的马的博客-CSDN博客_flink rocksdb](https://blog.csdn.net/wangxingxingalulu/article/details/121083421)

#### Managed Operator State

```
如果计算逻辑不需要通过Key做分类，可以用Operator State 
```

需实现 CheckpointedFunction 接口或者 ListCheckpinted 接口。

* CheckpointedFunction

需重写 snapshotState 和 initializeState

初始化 ：  构建StateDescriptor ，将状态注册到状态管理器

使用initializeState 方法会传入一个参数，为FunctionInitializationContext接口，建立FunctionInitializationContext后，在函数内部就可以通过上下文对状态进行操作和管理

```
若程序开启了检查点功能，任务会定时执行snapshotState方法，将上一次和这次检查点之间的状态进行更新
```

* ListCheckpointed

```
ListCheckpointed提供的方法只用于任务恢复
```

提供两个方法： snapshotState 和 restoreState

snapshotState 与 CheckpointedFunction的一致

restoreState ： 在任务失败后恢复上一个检查点中算子的状态

#### 比较

- [ ] Keyed State 只能应用于KeyedStream，而Operator State 都可以用
- [ ] Keyed State 可以理解成一个算子为每个子任务中的每个key维护了一个状态的命名空间，而所有子任务共享Operator State
- [ ] Operator State 只提供了ListState，而Keyed State 提供了 ValueState，ListState，ReducingState，MapState
- [ ] operatorStateStore的默认实现只有DefaultOperatorStateBackend，状态都存储在堆内存中，而Keyed State 的存储则根据存储状态介质配置的不同而不同





### 状态生存时间——TTl（Time-To-Live）

```
Flink 提供了状态的过期机制，TTL只对Keyed State有效
```

```
实际开发中，因状态过大导致做检查点的过程很慢甚至超时，有些算子状态(MapState)或中间值不需一致保存
```

1. 创建StateTtlConfig对象，配置相应参数，通过相应的StateDescriptor 调用enableTimeToLive 方法开启TTL特性;

2. 程序中设置了相应的keyedStateBackend后，任务第一次初始化会调用AbstractKeyedStateBackend 中的 getOrCreateKeyedState方法创建状态，其中 调用了TtlStateFactory类的createStateAndWrapWithTtlLfEnabled方法，正是这个方法创建了带TTl的状态；

3. 带生存时间的Keyed State 继承自 AbstractTtlState，执行获得状态值和更新状态值方法时，先调用accessCallback。

* StateTtlConfig 如果设置了增量过期，那么会在获取、更新状态的时候根据状态的增量过期设定 清处不符合要求的状态
* 如果没，回调函数内容为空

4. 经过accessCallback的过滤后，相应的stateBackend 再根据生存时间设置的过期时间清楚过期数据，最后返回符合要求的状态



## 检查点   --  checkpoint

```
持久存储：
可根据时间进行回放的数据源存储 —————— Kafka、RabbitMQ、Kinesis
持久存储来存放任务的状态  ————————HDFS、S3、GFS
```



### 执行过程

1. 任务初始化时，JobMaster会通过 SchedulerNG完成各种调度工作， SchedulerNG.startScheduling() 会调用ExecutionGraph的 *scheduleForExecution* 方法进行作业的运行规划。
2. *scheduleForExecution* 方法会判断作业状态是否 created -> running，负责检查点的JobStatusListener——CheckpointCoordinatorDeActivator，监听到任务状态变为 running，立即调用CheckpointCoordinator 触发*startCheckpintScheduler* 方法 进行检查点的调度。

```
状态的转换通过 transitionState 方法完成，在转换过程中会通知所有JobStatusListener 状态变更信息
```

3. startCheckpintScheduler 中 会触发一个定时任务 *ScheduledTrigger*

```
ScheduledTrigger 负责根据用户配置的时间间隔进行状态处理
```

4. *ScheduledTrigger* 拿到作业的所有Execution，判断要进行快照的任务是否都是 running状态，再判断操作是检查点还是保存点(savepoint)，最后轮询所有Execution，触发triggerCheckpoint
5. triggerCheckpoint 拿到运行Execution任务的LogicalSlot信息，得到此Slot所在TaskManager的TaskManagerGateway，调用triggerCheckpoint方法 <!-- TaskManagerGateway.triggerCheckpoint -->

6. TaskManagerGateway.triggerCheckpoint 本质上是执行 TaskExecutorGateway的triggerCheckpoint方法 ，方法里通过executionAttemptID得到具体任务，最后触发任务的triggerCheckpointBarrier方法，通过任务的AbstractInvokable 类执行triggerCheckpoint 。

```
在流任务中，所有任务都继承自StreamTask，而StreamTask继承自AbstractInvokable 类
```

7. SteamTask执行triggerCheckpoint，会将运行在此任务中的所有StreamOperator 取出，并轮询执行snapshotState。任务在进行快照的时候，将状态和响应的 metainfo 异步写入文件系统，然后返回相应的statehandle对象用作恢复

```
SnapshotState 根据用户配置的StateBackend进行状态的snapShot操作
```

8. 所有算子完成后告知JobManager



### 任务容错



Restart Strategy  和   Failover Strategy： 前者决定失败的任务是否应该重启，什么时候重启；后者决定哪些任务需要重启

```
无论哪种配置，任务出错后进行恢复的本质不变————Task拿到最近一个检查点的状态进行恢复
```

典型的任务恢复过程FlinkKafkaConsumerBase 为例：

```
	FlinkKafkaConsumerBase类继承自RichParallelSourceFunction 使用了CheckpointedFunction接口
```

重写方法：initializeState       /        snapshowState

​			

* 调用initializeState:

1. 通过FunctionInitializationContext 得到任务相应的OperatorStateStore

2. 根据传入的参数从OperatorStateStore中拿到最后一次成功检查点中的状态

```
相对于FlinkKafkaConsumerBase 拿出的是一个ListState类型，存储的是 Tuple2<KafkaTopicPartiton,Long>
```

3. 根据FunctionInitializationContext 判断这次initializeState是否是任务重启恢复操作，将ListState赋值给restoredState，供后面open方法用



* 执行open

```
有关任务的配置或加载操作在open完成
```



将状态加载到任务，让任务从状态断点处恢复运行

```
FlinkKafkaConsumerBase 将Kafka相应的分区位移点(offset) 信息从状态中恢复，继续从offset消费数据
```



* 任务并行度改变后状态的恢复  ———— 状态重分配

构建 StateAssignmentOperation 对象后 ，会调用assignState 方法：

<!-- 算子加载对应状态 -->

1. 判断并行度是否改变
2. 对于Operator State ，会对每一个名称的状态计算出每个子任务中的元素个数和，并进行轮询调度(round robin)分配
3. 对于Keyed State 的重新分配，根据新的并发度和最大并发度计算新的keyGroupRange 然后根据subtaskIndex 获取keyGroupRange, 最后获取到相应的keyStateHandle 并完成状态的切割

## 状态后端(State Backend)

```
存放任务状态的地方
```



- [ ] FsStateBackend

两次检查点之间将状态保存在内存中，决定了状态的更改非常快。

做检查点时，将内存的状态刷到文件系统

短板：如果遇到大状态任务，每次做检查点需将全量的状态写入文件系统，资源浪费<!-- 之前存储过的状态在下一次并不需要重复上传 -->，分配给TaskManager 的内存时提交任务时定好的，不能改变大小，某时间点任务处理的数据量猛增，会导致任务故障转移(failover)

- [ ] MemoryStateBackend

同FsStateBackend 两次检查点之间将状态保存在内存中，决定了状态的更改非常快。

做检查点时，所有TaskManager 的状态上报给JobManager 并存储在内存    =>    状态存储不可靠

需考虑JobManager 内存设置问题    

只会用在测试环境

- [ ] RocksDBStateBackend

无论状态变更还是检查点保存，都用RocksDB存储，稳定性有保障

支持增量检查点机制，每次只上传最新变更的状态

不断将变更的状态存储到RocksDB中   =>  同时利用内存和磁盘资源 =>   对大状态任务的支持特别好

由于每次更改都要存储到RocksDB   =>  频繁的序列 反序列化操作，如果遇到任务数据倾斜，在倾斜的几个带状态的子任务中，数据处理很慢，导致  **反压**



1. 所有的状态后端都是通过构造函数初始化创建的，RocksDBStateBackend 继承 AbstractStateBackend 调用 ConfigurableStateBackend接口。AbstractStateBackend  有两个方法 createKeyedStateBackend     createOperatorStateBackend, 分别创建KeyedStateBackend 和 OperatorStateBackend ，存储 Keyed State 和Operator State
2. 会在StreamTaskStateInitializerImp的streamOperatorStateContext 中间接调用 <!-- streamOperatorStateContext 会舒适化Stream Operator中所有与状态有关的操作 -->

3. streamOperatorStateContext() 在 initializeState() 中进行。<!-- 每个算子初始化时，就会完成与状态后端相关的全部设置 -->



* keyedStateBackend 

当不为空时，初始化一个 DefaultKeyedStateStore 对象，应用了 KeyedStateStore 接口，所有Keyed State 在获取状态值时会调用相应方法，最后一步都是 getPartitionedState 方法<!-- 实现在AbstractKeyedStateBackend ，调用getOrCreateKeyedState(),最后调用 TtlStateFactory 的 createStateAndWrapWithTtlIfEnabled()-->

```
getPartitionedState 方法 是利用不同状态后端进行Keyed State 初始化 入口
```

```
createStateAndWrapWithTtlIfEnabled() 根据是否设置了TTL而创建状态，是 初始化TtlStateFactory；不是 调用stateBackend的 createInternalState方法。
```

```
如果用的heap backend  createInternalState会为相应的State 创建 stateTable
       rocksdb backend                                       ColumnFamilyHandle
```

<!-- stateTable    ColumnFamilyHandle   为状态介质， Keyed State 的操作通过它们进行 -->



* Operator Backend

AbstractKeyedStateBackend  中调用StreamOperatorStaContext 获取 operatorStateBackend

operatorStateBackend初始化在DefaultOperatorStateBackend 类中，getListState() 用于状态创建和变更

所有OperatorState维护在registeredOperatorStates 中 <!-- Map 数据结构，键是状态名，值是PartitionableListState对象 -->

PartitionableListState中有各种对状态值得操作



# 任务提交与运行



*Flink on YARN 模式*

## 任务提交整体流程



<img src="C:\Users\龙星宇\Desktop\xue\flink_study\img\v2-c5c7936dd56609251c82def5063bbacc_r.jpg" alt="v2-c5c7936dd56609251c82def5063bbacc_r" style="zoom:200%;" />





## DAG转换



```
program --> StreamGraph --> JobGraph  -->  ExcutionGraph 
ExcutionGraph 中的ExecutionVertex 经Slot分配最终部署到TaskMannger
```

![20200828104029128](C:\Users\龙星宇\Desktop\xue\flink_study\img\20200828104029128.png)

![20200828104159395](C:\Users\龙星宇\Desktop\xue\flink_study\img\20200828104159395.png)

![20200924224241524](C:\Users\龙星宇\Desktop\xue\flink_study\img\20200924224241524.png)





### WordCount：

```java

package org.apache.flink.streaming.examples.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.examples.wordcount.util.WordCountData;
import org.apache.flink.util.Collector;

/**
 * Implements the "WordCount" program that computes a simple word occurrence
 * histogram over text files in a streaming fashion.
 *
 * <p>The input is a plain text file with lines separated by newline characters.
 *
 * <p>Usage: <code>WordCount --input &lt;path&gt; --output &lt;path&gt;</code><br>
 * If no parameters are provided, the program is run with default data from
 * {@link WordCountData}.
 *
 * <p>This example shows how to:
 * <ul>
 * <li>write a simple Flink Streaming program,
 * <li>use tuple data types,
 * <li>write and use user-defined functions.
 * </ul>
 */
public class WordCount {

	// *************************************************************************
	// PROGRAM
	// *************************************************************************

	public static void main(String[] args) throws Exception {

		// Checking input parameters
		final ParameterTool params = ParameterTool.fromArgs(args);

		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// make parameters available in the web interface
		env.getConfig().setGlobalJobParameters(params);

		// get input data
		DataStream<String> text;
		if (params.has("input")) {
			// read the text file from given input path
			text = env.readTextFile(params.get("input"));
		} else {
			System.out.println("Executing WordCount example with default input data set.");
			System.out.println("Use --input to specify file input.");
			// get default test text data
			text = env.fromElements(WordCountData.WORDS);
		}

		DataStream<Tuple2<String, Integer>> counts =
			// split up the lines in pairs (2-tuples) containing: (word,1)
			text.flatMap(new Tokenizer())
			// group by the tuple field "0" and sum up tuple field "1"
			.keyBy(0).sum(1);

		// emit result
		if (params.has("output")) {
			counts.writeAsText(params.get("output"));
		} else {
			System.out.println("Printing result to stdout. Use --output to specify output path.");
			counts.print();
		}

		// execute program
		env.execute("Streaming WordCount");
	}

	// *************************************************************************
	// USER FUNCTIONS
	// *************************************************************************

	/**
	 * Implements the string tokenizer that splits sentences into words as a
	 * user-defined FlatMapFunction. The function takes a line (String) and
	 * splits it into multiple pairs in the form of "(word,1)" ({@code Tuple2<String,
	 * Integer>}).
	 */
	public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
			// normalize and split the line
			String[] tokens = value.toLowerCase().split("\\W+");

			// emit the pairs
			for (String token : tokens) {
				if (token.length() > 0) {
					out.collect(new Tuple2<>(token, 1));
				}
			}
		}
	}

}

```





### 算子    -->   StreamGraph



DataStream 转换过程会把算子封装成StreamTransformation ，放到StreamExecutionEnvironment 的变量Transformation中，StreamTransformation 本身有前一个Transform 引用。这样用户的转换逻辑就全部放到Transformations 中。

生成StreamGraph就是把Transformations 转换成StreamGraph ：

StreamTransformation 生成StreamGraph 就是构造StreamNode ，StreamNode 包含当前算子的上下游关系。

每个StreamTransformation 包含的算子构造一个StreamNode，StreamTransformation 包含的上下游关系构造StreamEdge

```
StreamGraphGenerator 的transform 方法 在构造StreamNode时会设置Slot共享组
```

<!-- Slot共享组时分配Slot时的依据，Flink 可以把不同子任务分配到相同的Slot中运行，充分利用资源，相同的Slot共享组可以被分配到同一个Slot。如果不设置，默认时default -->



 ### StreamGraph  -->  JobGraph

```
StreamGraph 转换为JobGraph 就是构建JobVertex过程， JobVertex是后续Flink任务的最小调度单位
```

JobVertex 可以包括多个算子，也就是把多个算子根据一定规则串联起来。

创建JobGraph 主要由 StreamingJobGraphGenerator 的createJobGraph 完成：

1. 遍历StreamGraph，为每个streamNode 生成byte 数组类型的哈希值并赋值给OperatorID，作为 状态恢复的唯一标识
2. 利用StreamNode及相关关系构造 JobVertex<!-- StreamingJobGraphGenerator.createChain -->

* 不能串联到一起的，单独生成JobVertex，并把算子中的用户函数（如WordCount的Tokenizer方法）及相关属性序列化到JobVertex的configuration 中
* 可以串联到一起的，选取串开头的StreamNode 作为当前JobVertex 的JobVertexID，将其他StreamNode 序列化到配置字段 chainedTaskConfig_ 中。序列化的对象也是存储了StreamNode 相关信息的StreamConfig类。算子之间的关系生成了JobEdge 和 IntermediateDataSet 类，放到JobVertex中

3. 设置Slot共享组及其他作业相关的属性，包括资源分配location属性，checkpoint等



### 算子链

[深入解析 Flink 的算子链机制 - 云+社区 - 腾讯云 (tencent.com)](https://cloud.tencent.com/developer/article/1751614)

```
算子链：所有 chain 在一起的 sub-task 都会在同一个线程（即 TaskManager 的 slot）中执行，能够减少不必要的数据交换、序列化和上下文切换，从而提高作业的执行效率。
```

<img src="C:\Users\龙星宇\Desktop\xue\flink_study\img\chain.png" alt="chain" style="zoom: 80%;" />



<img src="C:\Users\龙星宇\Desktop\xue\flink_study\img\chain1.png" alt="chain1" style="zoom:80%;" />

```
算子链是在优化逻辑计划时加入的，也就是由 StreamGraph 生成 JobGraph 的过程中
```



```java
private JobGraph createJobGraph() {
    // make sure that all vertices start immediately
    jobGraph.setScheduleMode(streamGraph.getScheduleMode());
    // Generate deterministic hashes for the nodes in order to identify them across
    // submission iff they didn't change.
    Map<Integer, byte[]> hashes = defaultStreamGraphHasher.traverseStreamGraphAndGenerateHashes(streamGraph);
    // Generate legacy version hashes for backwards compatibility
    List<Map<Integer, byte[]>> legacyHashes = new ArrayList<>(legacyStreamGraphHashers.size());
    for (StreamGraphHasher hasher : legacyStreamGraphHashers) {
        legacyHashes.add(hasher.traverseStreamGraphAndGenerateHashes(streamGraph));
    }
    Map<Integer, List<Tuple2<byte[], byte[]>>> chainedOperatorHashes = new HashMap<>();
    setChaining(hashes, legacyHashes, chainedOperatorHashes);

    setPhysicalEdges();
    // 略......

    return jobGraph;
}
```

该方法会先计算出 StreamGraph 中各个节点的哈希码作为唯一标识，并创建一个空的 Map 结构保存即将被链在一起的算子的哈希码，然后调用 setChaining() 方法

```java
private void setChaining(Map<Integer, byte[]> hashes, List<Map<Integer, byte[]>> legacyHashes, Map<Integer, List<Tuple2<byte[], byte[]>>> chainedOperatorHashes) {
    for (Integer sourceNodeId : streamGraph.getSourceIDs()) {
        createChain(sourceNodeId, sourceNodeId, hashes, legacyHashes, 0, chainedOperatorHashes);
    }
}
```

逐个遍历 StreamGraph 中的 Source 节点，并调用 createChain() 方法

```java

private List<StreamEdge> createChain(
        Integer startNodeId,
        Integer currentNodeId,
        Map<Integer, byte[]> hashes,
        List<Map<Integer, byte[]>> legacyHashes,
        int chainIndex,
        Map<Integer, List<Tuple2<byte[], byte[]>>> chainedOperatorHashes) {
    if (!builtVertices.contains(startNodeId)) {
        List<StreamEdge> transitiveOutEdges = new ArrayList<StreamEdge>();
        List<StreamEdge> chainableOutputs = new ArrayList<StreamEdge>();
        List<StreamEdge> nonChainableOutputs = new ArrayList<StreamEdge>();

        StreamNode currentNode = streamGraph.getStreamNode(currentNodeId);
        for (StreamEdge outEdge : currentNode.getOutEdges()) {
            if (isChainable(outEdge, streamGraph)) {
                chainableOutputs.add(outEdge);
            } else {
                nonChainableOutputs.add(outEdge);
            }
        }

        for (StreamEdge chainable : chainableOutputs) {
            transitiveOutEdges.addAll(
                    createChain(startNodeId, chainable.getTargetId(), hashes, legacyHashes, chainIndex + 1, chainedOperatorHashes));
        }

        for (StreamEdge nonChainable : nonChainableOutputs) {
            transitiveOutEdges.add(nonChainable);
            createChain(nonChainable.getTargetId(), nonChainable.getTargetId(), hashes, legacyHashes, 0, chainedOperatorHashes);
        }

        List<Tuple2<byte[], byte[]>> operatorHashes =
            chainedOperatorHashes.computeIfAbsent(startNodeId, k -> new ArrayList<>());

        byte[] primaryHashBytes = hashes.get(currentNodeId);
        OperatorID currentOperatorId = new OperatorID(primaryHashBytes);

        for (Map<Integer, byte[]> legacyHash : legacyHashes) {
            operatorHashes.add(new Tuple2<>(primaryHashBytes, legacyHash.get(currentNodeId)));
        }

        chainedNames.put(currentNodeId, createChainedName(currentNodeId, chainableOutputs));
        chainedMinResources.put(currentNodeId, createChainedMinResources(currentNodeId, chainableOutputs));
        chainedPreferredResources.put(currentNodeId, createChainedPreferredResources(currentNodeId, chainableOutputs));

        if (currentNode.getInputFormat() != null) {
            getOrCreateFormatContainer(startNodeId).addInputFormat(currentOperatorId, currentNode.getInputFormat());
        }
        if (currentNode.getOutputFormat() != null) {
            getOrCreateFormatContainer(startNodeId).addOutputFormat(currentOperatorId, currentNode.getOutputFormat());
        }

        StreamConfig config = currentNodeId.equals(startNodeId)
                ? createJobVertex(startNodeId, hashes, legacyHashes, chainedOperatorHashes)
                : new StreamConfig(new Configuration());

        setVertexConfig(currentNodeId, config, chainableOutputs, nonChainableOutputs);

        if (currentNodeId.equals(startNodeId)) {
            config.setChainStart();
            config.setChainIndex(0);
            config.setOperatorName(streamGraph.getStreamNode(currentNodeId).getOperatorName());
            config.setOutEdgesInOrder(transitiveOutEdges);
            config.setOutEdges(streamGraph.getStreamNode(currentNodeId).getOutEdges());
            for (StreamEdge edge : transitiveOutEdges) {
                connect(startNodeId, edge);
            }
            config.setTransitiveChainedTaskConfigs(chainedConfigs.get(startNodeId));
        } else {
            chainedConfigs.computeIfAbsent(startNodeId, k -> new HashMap<Integer, StreamConfig>());
            config.setChainIndex(chainIndex);
            StreamNode node = streamGraph.getStreamNode(currentNodeId);
            config.setOperatorName(node.getOperatorName());
            chainedConfigs.get(startNodeId).put(currentNodeId, config);
        }

        config.setOperatorID(currentOperatorId);
        if (chainableOutputs.isEmpty()) {
            config.setChainEnd();
        }
        return transitiveOutEdges;
    } else {
        return new ArrayList<>();
    }
}
```

3 个 List 结构：

* transitiveOutEdges：当前算子链在 JobGraph 中的出边列表，同时也是 createChain() 方法的最终返回值

* chainableOutputs：当前能够链在一起的 StreamGraph 边列表

* nonChainableOutputs：当前不能够链在一起的 StreamGraph 边列表



从 Source 开始遍历 StreamGraph 中当前节点的所有出边<!-- StreamEdge -->

调用 isChainable() <!-- 判断是否可以被链在一起-->

对于 chainableOutputs 中的边，就会以这些边的直接下游为起点，继续递归调用createChain() 方法延展算子链。

对于 nonChainableOutputs 中的边，由于当前算子链的延展已经到头，就会以这些“断点”为起点，继续递归调用 createChain() 方法试图创建新的算子链。

```
也就是说，逻辑计划中整个创建算子链的过程都是递归的，亦即实际返回时，是从 Sink 端开始返回的。
```

判断当前节点是不是算子链的起始节点。如果是，则调用 createJobVertex()方法为算子链创建一个 JobVertex（ 即 JobGraph 中的节点），也就形成了我们在Web UI 中看到的 JobGraph 效果

最后，还需要将各个节点的算子链数据写入各自的 StreamConfig 中，算子链的起始节点要额外保存下 transitiveOutEdges。

isChainable():

```java
public static boolean isChainable(StreamEdge edge, StreamGraph streamGraph) {
    StreamNode upStreamVertex = streamGraph.getSourceVertex(edge);
    StreamNode downStreamVertex = streamGraph.getTargetVertex(edge);

    StreamOperatorFactory<?> headOperator = upStreamVertex.getOperatorFactory();
    StreamOperatorFactory<?> outOperator = downStreamVertex.getOperatorFactory();

    return downStreamVertex.getInEdges().size() == 1
            && outOperator != null
            && headOperator != null
            && upStreamVertex.isSameSlotSharingGroup(downStreamVertex)
            && outOperator.getChainingStrategy() == ChainingStrategy.ALWAYS
            && (headOperator.getChainingStrategy() == ChainingStrategy.HEAD ||
                headOperator.getChainingStrategy() == ChainingStrategy.ALWAYS)
            && (edge.getPartitioner() instanceof ForwardPartitioner)
            && edge.getShuffleMode() != ShuffleMode.BATCH
            && upStreamVertex.getParallelism() == downStreamVertex.getParallelism()
            && streamGraph.isChainingEnabled();
}
```



- 上下游算子实例处于同一个 SlotSharingGroup 中
- 下游算子的链接策略（ChainingStrategy）为 ALWAYS ——既可以与上游链接，也可以与下游链接。我们常见的 map()、filter() 等都属此类
- 上游算子的链接策略为 HEAD 或 ALWAYS。HEAD 策略表示只能与下游链接，这在正常情况下是 Source 算子的专属
- 两个算子间的物理分区逻辑是 ForwardPartitioner 
- 两个算子间的 shuffle 方式不是批处理模式
- 上下游算子实例的并行度相同
- 没有禁用算子链

```
用户可以在一个算子上调用 startNewChain() 方法强制开始一个新的算子链，或者调用 disableOperatorChaining() 方法指定它不参与算子链。
```

<!-- 如果要在整个运行时环境中禁用算子链，调用 StreamExecutionEnvironment.disableOperatorChaining() 即可。-->

在 JobGraph 转换成 ExecutionGraph 并交由 TaskManager 执行之后，会生成调度执行的基本任务单元 ——StreamTask，负责执行具体的 StreamOperator 逻辑。在StreamTask.invoke() 方法中，初始化了状态后端、checkpoint 存储和定时器服务之后，可以发现：

```java
operatorChain = new OperatorChain<>(this, recordWriters);
headOperator = operatorChain.getHeadOperator();
```

- headOperator：算子链的第一个算子，对应 JobGraph 中的算子链起始节点；
- allOperators：算子链中的所有算子，倒序排列，即 headOperator 位于该数组的末尾；
- streamOutputs：算子链的输出，可以有多个；
- chainEntryPoint：算子链的“入口点”

由上可知，所有 StreamTask 都会创建 OperatorChain。如果一个算子无法进入算子链，也会形成一个只有 headOperator 的单个算子的 OperatorChain。

OperatorChain:

```java

for (int i = 0; i < outEdgesInOrder.size(); i++) {
    StreamEdge outEdge = outEdgesInOrder.get(i);
    RecordWriterOutput<?> streamOutput = createStreamOutput(
        recordWriters.get(i),
        outEdge,
        chainedConfigs.get(outEdge.getSourceId()),
        containingTask.getEnvironment());
    this.streamOutputs[i] = streamOutput;
    streamOutputMap.put(outEdge, streamOutput);
}

// we create the chain of operators and grab the collector that leads into the chain
List<StreamOperator<?>> allOps = new ArrayList<>(chainedConfigs.size());
this.chainEntryPoint = createOutputCollector(
    containingTask,
    configuration,
    chainedConfigs,
    userCodeClassloader,
    streamOutputMap,
    allOps);

if (operatorFactory != null) {
    WatermarkGaugeExposingOutput<StreamRecord<OUT>> output = getChainEntryPoint();
    headOperator = operatorFactory.createStreamOperator(containingTask, configuration, output);
    headOperator.getMetricGroup().gauge(MetricNames.IO_CURRENT_OUTPUT_WATERMARK, output.getWatermarkGauge());
} else {
    headOperator = null;
}

// add head operator to end of chain
allOps.add(headOperator);
this.allOperators = allOps.toArray(new StreamOperator<?>[allOps.size()]);
```

首先会遍历算子链整体的所有出边，并调用 createStreamOutput() 方法创建对应的下游输出 RecordWriterOutput。然后就会调用 createOutputCollector() 方法创建物理的算子链，并返回 chainEntryPoint，这个方法比较重要，部分代码如下。

```java

private <T> WatermarkGaugeExposingOutput<StreamRecord<T>> createOutputCollector(
        StreamTask<?, ?> containingTask,
        StreamConfig operatorConfig,
        Map<Integer, StreamConfig> chainedConfigs,
        ClassLoader userCodeClassloader,
        Map<StreamEdge, RecordWriterOutput<?>> streamOutputs,
        List<StreamOperator<?>> allOperators) {
    List<Tuple2<WatermarkGaugeExposingOutput<StreamRecord<T>>, StreamEdge>> allOutputs = new ArrayList<>(4);

    // create collectors for the network outputs
    for (StreamEdge outputEdge : operatorConfig.getNonChainedOutputs(userCodeClassloader)) {
        @SuppressWarnings("unchecked")
        RecordWriterOutput<T> output = (RecordWriterOutput<T>) streamOutputs.get(outputEdge);
        allOutputs.add(new Tuple2<>(output, outputEdge));
    }

    // Create collectors for the chained outputs
    for (StreamEdge outputEdge : operatorConfig.getChainedOutputs(userCodeClassloader)) {
        int outputId = outputEdge.getTargetId();
        StreamConfig chainedOpConfig = chainedConfigs.get(outputId);
        WatermarkGaugeExposingOutput<StreamRecord<T>> output = createChainedOperator(
            containingTask,
            chainedOpConfig,
            chainedConfigs,
            userCodeClassloader,
            streamOutputs,
            allOperators,
            outputEdge.getOutputTag());
        allOutputs.add(new Tuple2<>(output, outputEdge));
    }
    // 以下略......
}
```

该方法从上一节提到的 StreamConfig 中分别取出出边和链接边的数据，并创建各自的 Output。出边的 Output 就是将数据发往算子链之外下游的 RecordWriterOutput，而链接边的输出要靠 createChainedOperator() 方法。

```java

private <IN, OUT> WatermarkGaugeExposingOutput<StreamRecord<IN>> createChainedOperator(
        StreamTask<?, ?> containingTask,
        StreamConfig operatorConfig,
        Map<Integer, StreamConfig> chainedConfigs,
        ClassLoader userCodeClassloader,
        Map<StreamEdge, RecordWriterOutput<?>> streamOutputs,
        List<StreamOperator<?>> allOperators,
        OutputTag<IN> outputTag) {
    // create the output that the operator writes to first. this may recursively create more operators
    WatermarkGaugeExposingOutput<StreamRecord<OUT>> chainedOperatorOutput = createOutputCollector(
        containingTask,
        operatorConfig,
        chainedConfigs,
        userCodeClassloader,
        streamOutputs,
        allOperators);

    // now create the operator and give it the output collector to write its output to
    StreamOperatorFactory<OUT> chainedOperatorFactory = operatorConfig.getStreamOperatorFactory(userCodeClassloader);
    OneInputStreamOperator<IN, OUT> chainedOperator = chainedOperatorFactory.createStreamOperator(
            containingTask, operatorConfig, chainedOperatorOutput);

    allOperators.add(chainedOperator);

    WatermarkGaugeExposingOutput<StreamRecord<IN>> currentOperatorOutput;
    if (containingTask.getExecutionConfig().isObjectReuseEnabled()) {
        currentOperatorOutput = new ChainingOutput<>(chainedOperator, this, outputTag);
    }
    else {
        TypeSerializer<IN> inSerializer = operatorConfig.getTypeSerializerIn1(userCodeClassloader);
        currentOperatorOutput = new CopyingChainingOutput<>(chainedOperator, inSerializer, outputTag, this);
    }

    // wrap watermark gauges since registered metrics must be unique
    chainedOperator.getMetricGroup().gauge(MetricNames.IO_CURRENT_INPUT_WATERMARK, currentOperatorOutput.getWatermarkGauge()::getValue);
    chainedOperator.getMetricGroup().gauge(MetricNames.IO_CURRENT_OUTPUT_WATERMARK, chainedOperatorOutput.getWatermarkGauge()::getValue);
    return currentOperatorOutput;
}
```

我们一眼就可以看到，这个方法递归调用了上述 createOutputCollector() 方法，与逻辑计划阶段类似，通过不断延伸 Output 来产生 chainedOperator（即算子链中除了headOperator 之外的算子），并逆序返回，这也是 allOperators 数组中的算子顺序为倒序的原因。

chainedOperator 产生之后，将它们通过 ChainingOutput 连接起来，形成如下图所示的结构。

![7zcmtqthnd](C:\Users\龙星宇\Desktop\xue\flink_study\img\7zcmtqthnd.png)

```   
逆序 <= 类似递归，最底层先return     ?
```



ChainingOutput.collect() 方法是如何输出数据流的

```java

@Override
public void collect(StreamRecord<T> record) {
    if (this.outputTag != null) {
        // we are only responsible for emitting to the main input
        return;
    }
    pushToOperator(record);
}

@Override
public <X> void collect(OutputTag<X> outputTag, StreamRecord<X> record) {
    if (this.outputTag == null || !this.outputTag.equals(outputTag)) {
        // we are only responsible for emitting to the side-output specified by our
        // OutputTag.
        return;
    }
    pushToOperator(record);
}

protected <X> void pushToOperator(StreamRecord<X> record) {
    try {
        // we know that the given outputTag matches our OutputTag so the record
        // must be of the type that our operator expects.
        @SuppressWarnings("unchecked")
        StreamRecord<T> castRecord = (StreamRecord<T>) record;
        numRecordsIn.inc();
        operator.setKeyContextElement1(castRecord);
        operator.processElement(castRecord);
    }
    catch (Exception e) {
        throw new ExceptionInChainedOperatorException(e);
    }
}
```

可见是通过调用链接算子的 processElement() 方法，直接将数据推给下游处理了。也就是说，OperatorChain 完全可以看做一个由 headOperator 和 streamOutputs组成的单个算子，其内部的 chainedOperator 和 ChainingOutput 都像是被黑盒遮蔽，同时没有引入任何 overhead。打通了算子链在执行层的逻辑，看官应该会明白 chainEntryPoint 的含义了。由于它位于递归返回的终点，所以它就是流入算子链的起始 Output，即上图中指向 headOperator 的 RecordWriterOutput。









### JobGraph  -->  ExecutionGraph

```
ExecutionGraph 是JobGraph的并发版本，每个JobVertex对应ExecutionJobVertex。
```

<!-- ExecutionJobVertex 就是 JobVertex增加一些执行信息的封装类 -->

一个有10个并发的算子会生成1个JobVertex，1个ExecutionJobVertex和10个ExecutionVertex。

<!-- ExecutionVertex表示一个并发的子任务，可以被执行一次或多次，内部Execution对象表示执行状态 -->

ExecutionGraph 内部也有JobStatus 记录整个作业的执行状态。

```
ExecutionVertex 通过IntermediateRusultPartition 连接
```

根据JobVertex 构建ExecutionJobVertex，根据IntermediateDataSet 构建IntermediateResult

```
构建入口在ExecutionGraphBuilder的buildGraph方法中，主要逻辑实现在ExecutionGraph的attachJobGraph方法中
```



### ExecutionGraph   --> Task

```
Slot分配 部署
```





## Slot分配



### 相关逻辑角色

#### SlotManager  SlotPool



#### PhysicalSlot  LogicalSlot  MuliTaskSlot  SingleTaskSlot

```
前两个是用来抽象Slot概念的，后两个是用来辅助Slot的分配而用到的包装类 辅助共享Slot分配(Slot共享组)
```

* PhysicalSlot ： 物理意义上的Slot，已经分配了唯一标识AllocationID 拥有TaskManagerGateway等属性，可以用来部署任务
* LogicalSlot ：逻辑意义上的Slot，一个LogicalSlot对应一个ExecutionVertex或任务，或者多个LogicalSlot对应一个PhysicalSlot，表示它们共用同一个Slot执行

```
PhysicalSlot 唯一实现类：AllocatedSlot
LogicalSlot  主要实现类：SingleLogicalSlot
都实现了tryAssignPayload()，AllocatedSlot 可以装载一个SingleLogicalSlot，SingleLogicalSlot可以装载一个Execution(Execution表示ExecutionVertex的一次执行)        payload - "被分配给"
=>       Execution拥有一个SingleLogicalSlot，SingleLogicalSlot拥有AllocatedSlot,AllocatedSlot包含Slot 的物理信息，如TaskManagerGateway，可以用来执行一次Execution
```

* MultiTaskSlot：完成多个LogicalSlot 对一个PhysicalSlot 的映射 用到的工具类

<!-- MultiTaskSlot SingleTaskSlot 接口都是TaskSlot -->

MultiTaskSlot 树形结构，叶子节点是SingleTaskSlot，非叶子节点和根 是 MultiTaskSlot，根节点分配一个SlotContext <!-- SlotContext具体实现就是AllocatedSlot，也就是PhysicalSlot -->，所有叶子节点共享这个physicalSlot，每个叶子节点对应一个SingleLogicalSlot ，也就是LogicalSlot   =>      利用该结构表达多个LogicalSlot对一个PhysicalSlot的映射

```
每个叶子节点都有唯一的AbstractID，就是JobVertexID =>  每个物理Slot节点上执行的任务是不同的，不可能同一个任务的并发执行在相同的Slot上
```



### Slot申请分配流程



Slot在ResourceManager和TaskManager之间申请







```
JobMaster 负责任务的调度与部署。入口方法是 startScheduling()，JobMaster会委托给LegacyScheduler执行
LegacyScheduler 是ExecutionGraph的一个门面类，具体实现通过ExecutionGraph，主要方法是ExecutionGraph的scheduleForExecution 
```

*作业的调度与部署是以 ExecutionVertex 为单位进行*



根据不同ScheduleMode 进入不同方法：

* LAZY_FROM_SOURCES：下游的任务需要在上游结果产生的前提下进行调度，用在离线场景
* LAZY_FROM_SOURCES_WITH_BATCH_SLOT_REQUEST：与LAZY_FROM_SOURCES基本一致，支持在Slot资源不足的情况下执行作业，但需确保作业中没有shuffle
* EAGER：立刻调度所有任务，流任务一般采用这种模式

SchedulingUtils.scheduleEager()：

- [ ] allocateResourcesForExecution    --> Slot的分配

### Slot分配

* allocateResourcesForExecution：

分配前先调用 calculatePreferredLocations 寻找Slot的位置偏好

进入SchedulerImpl类的allocateSlot

```
SchedulerImp类 会根据是否有Slot共享组设置调用不同方法   --> allocateSharedSlot
```

<!-- 及时用户不设置Slot共享组，也有默认的Slot共享组(default) -->

* allocateSharedSlot(): 

方法核心是分配MultiTaskSlot

* allocateMultiTaskSlot();

1. 从resolvedRootSlots 中寻找可共享的、已分配Slot的MultiTaskSlot

```
是否符合共享的过滤条件：所有节点的groupID 与当前需要分配的groupID不同  =>    同一个物理Slot上不能有相同的任务
```

2. 找到后，且符合要求的位置偏好，直接返回
3. 没找到可共享的MultiTaskSlot ，从slotPool中分配一个PhysicalSlot，在符合位置偏好的情况下新生成一个MultiTaskSlot，将新创建的rootMultiTaskSlot作为PhysicalSlot的装载
4. 如果没申请到新的PhysicalSlot，检查slotSharingManager 的 unresolvedRootSlots 中是否有符合要求的MultiTaskSlot，有则返回
5. 若还是没有，slotPool向ResourceManager 请求新的Slot，然后新建一个MultiTaskSlot，装载后返回

<!-- unresolvedRootSlots ：还未分配物理资源的Slot(SlotContext的Future还未完成) -->

* calculatePreferredLocations

```
Slot位置偏好的选择
```

ExecutionVertex 的 getPreferredLocations 方法获取所有的偏好信息

根据偏好模式选择偏好信息

locationPreferenceConstraint 是 All   -->  拿到前面获取的所有偏好信息

locationPreferenceConstraint 是 ANY -->  选取已经分配好的偏好信息



* getPreferredLocations

如果有历史状态，直接拿历史状态的偏好信息

```
作业从检查点恢复，将上次的位置信息作为本次的位置偏好
```

无，使用getPreferredLocationsBasedOnInputs 获取的偏好信息



* getPreferredLocationsBasedOnInputs

若该ExecutionVertex没有上游 (source)，返回空集合，表示无位置偏好

根据输入来选择位置偏好，但如果某个输入的位置太多(>8)，该输入不能作为位置偏好依据

```
根据已有的信息选择；如果没有，依据上游的位置信息选择；如果上游分布的范围很广，那么位置信息没有参考价值，选任何一个位置都没太大影响
```



```java
	/**
	 * Gets the location preferences of the vertex's current task execution, as determined by the locations
	 * of the predecessors from which it receives input data.
	 * If there are more than MAX_DISTINCT_LOCATIONS_TO_CONSIDER different locations of source data, this
	 * method returns {@code null} to indicate no location preference.
	 *
	 * @return The preferred locations based in input streams, or an empty iterable,
	 *         if there is no input-based preference.
	 */
	public Collection<CompletableFuture<TaskManagerLocation>> getPreferredLocationsBasedOnInputs() {
		// otherwise, base the preferred locations on the input connections
		if (inputEdges == null) {
			return Collections.emptySet();
		}
		else {
			Set<CompletableFuture<TaskManagerLocation>> locations = new HashSet<>(getTotalNumberOfParallelSubtasks());
			Set<CompletableFuture<TaskManagerLocation>> inputLocations = new HashSet<>(getTotalNumberOfParallelSubtasks());

			// go over all inputs
			for (int i = 0; i < inputEdges.length; i++) {
				inputLocations.clear();
				ExecutionEdge[] sources = inputEdges[i];
				if (sources != null) {
					// go over all input sources
					for (int k = 0; k < sources.length; k++) {
						// look-up assigned slot of input source
						CompletableFuture<TaskManagerLocation> locationFuture = sources[k].getSource().getProducer().getCurrentTaskManagerLocationFuture();
						// add input location
						inputLocations.add(locationFuture);
						// inputs which have too many distinct sources are not considered
						if (inputLocations.size() > MAX_DISTINCT_LOCATIONS_TO_CONSIDER) {
							inputLocations.clear();
							break;
						}
					}
				}
				// keep the locations of the input with the least preferred locations
				if (locations.isEmpty() || // nothing assigned yet
						(!inputLocations.isEmpty() && inputLocations.size() < locations.size())) {
					// current input has fewer preferred locations
					locations.clear();
					locations.addAll(inputLocations);
				}
			}

			return locations.isEmpty() ? Collections.emptyList() : locations;
		}
	}
```

flink 偏好位置信息生成依据
 flink source task 所在节点，并作为consumer task的偏好位置，consumer task优先调度到source task所在节点

 `gets the location preferences of the vertex's current task execution, as determined by the locationsof the predecessors from which it receives input data.`





- [ ] execution.deploy()      -->   Slot分配后Task的部署

### 任务部署

* execution.deploy() 

拿到分配的Slot构造TaskDeploymentDescriptor，然后通过TaskManagerGateway 进行提交

```
TaskDeploymentDescriptor 包装了执行任务所需的大部分信息，且都经过序列化
```



  

## 任务执行机制

```
ExecutionVertex 经过Slot分配后进行部署，TaskManager收到submitTask 请求，启动并执行任务
```

入口在TaskExecutor 的submitTask方法

### 执行过程

1. 初始化StreamTask

任务启动后构造StreamTask ，调用invoke()。

```
StreamTask是流作业的执行基类，是调度和执行的基本单元和实现类
```

* invoke()

初始化stateBackend，加载operatorChain



执行init方法，抽象方法，<!-- OneInputStreamTask(StreamTask的实现类  -- 一个输入的任务)，init的主要工作是构建InputGate，用来消费分区的数据 -->



初始化算子状态(从检查点恢复数据)，然后打开所有算子，最终会调用ProcessFunction的open方法，就是用户实现的函数的open方法



开始运行算子，输入数据处理完成后，进入close流程



关闭过程包括：timerService 停止服务 (用来注册Timer)，关闭所有的算子，清空所有缓存的数据，清理所有算子



执行一个finally步骤，包括停止timerService，停止异步检查点进程，关闭recordWriter



2. 执行处理方法

```
早期版本StreamTask的run 是抽象方法，1.9 变为具体方法，将performDefaultAction 让各实现类实现，新增mailbox变量--> 对线程模型的优化
performDefaultAction  是各个StreamTask要实现的具体方法，是算子的主要工作流程入口
```

3. 拉取数据

具体实现：SourceStreamTask

SourceStreamTask的performDefaultAction方法 最终启动SourceFunction的run方法，对于流任务，SourceFunction永不休止的进行数据的消费或生产

4. 发送数据

数据在SourceFunction中产生后，主线程调用sourceContext 进行收集，经过output接口实现类将其发送到网络端或下游算子

Output实现类

* RecordWriterOutput：将数据通过RecordWriter 发送出去
* ChainingOutput：将数据推送到下一个算子

5. 处理数据

具体实现：OneInputSteamTask 

OneInputSteamTask 的 performDefaultAction方法 就是调用 StreamOneInputProcessor的processInput，然后调用算子的processElement

```
一个算子基础类：AbstractUdfSteamOperator  ，可以接受用户定义函数(UDF)的算子类
```

AbstractUdfSteamOperator的processElement会调用userFunction的具体方法，也就是用户实现的方法

```
对于MapFunction就是调用Map方法，FlatMapFunction就是FlatMap方法，SInkFunction就是invoke方法
```

6. 算子串处理数据

如果当前OneInputSteamTask 的算子是一个算子串，经过第一个算子的processElement方法后，ChainingOutput会调用collect方法把数据推送到下一个算子

7. 将数据发送到外部

如果当前算子是StreamSink，那么userFunction就是SinkFunction，最后调用invoke方法，将数据发送到外部系统



## MailBox线程模型



```
MailBox 是1.9版本引入的任务线程模型
```

### WHY

1.4版本中：

SteamTask内部有一个Object类型的锁变量lock，用来同步算子处理数据和检查点、定时器触发等操作

lock被多个地方引用和使用，通过SourceContext的API暴露给了用户

* 锁对象在多个类中传递和使用，代码的可读性和后期的维护成本都是问题，且后续开发的功能容易因锁的使用不当而出现问题
* 把框架内部的锁暴露给用户，不是一个好的设计

### WHAT

借鉴Actor模型的设计理念，把需要同步的行为(action) 放到一个队列或消息容器，然后单线程顺序获取行为，然后执行

### HOW

用一个ringBuffer的Runnable数组缓存行为，然后实现take、put相关方法

MailboxProcessor 是核心门面类，提供主线程入口方法 runMailboxLoop

* runMailboxLoop

主要工作是处理MailBox消息，如果全部处理完成，且当前有输入数据或结果数据可以写出，执行默认行为，即调用算子的数据处理方法

MailboxProcessor 内封装了MailBox的核心实现 TaskMailBox，具体实现可以认为是BlockingQueue，用来存放Mail(内部封装了具体的行为，比如检查点行为)

MailboxProcessor内部还有一个MailboxDefaultAction，存放默认的数据处理行为，即算子处理数据的行为

MailboxExecutor 对外暴露submit方法(将Mail放到TaskMailBox)，存放在MailboxProcessor中

![MailBox](C:\Users\龙星宇\Desktop\xue\flink_study\img\MailBox.png)



* actionExecutor 变量：

兼容以前getCheckpointLock方法而引入的辅助类，用来兼容历史遗留的同步锁被传入SourceContext的情况

* SourceFunction一般是新启动一个线程，无限循环的获取或者生产数据，导致SourceStreamTask的processInput 无法返回主线程或者第二次调用不合法



> ```
> flink之前是通过一个CheckPointLock加锁来实现多线程之间的互斥操作，之后引入Actor模型的mailbox机制来取代现有的多线程模型。变为了单线程(mailboxprocessor)+阻塞队列(mailbox)的形式。flink重构核心线程模型，所有算子的Task都是StreamTask，Source算子由于历史原因是SourceStreamTask（兼容的核心思想是以两个不同的线程来独立运行，SourceFunction对应的事件生成在一个线程上，而Mailbox是另一个线程，并且两者以检查点锁来保持互斥）
> 这样针对遗留的Source循环还是以独立的一套机制运行，而绝大部分算子的task则运行在mailbox线程上。
> ```



> 除了常规的处理逻辑，还有其他线程可能会访问Task线程数据，比如
>
> - **checkpoint**：checkpoint的同步阶段
> - **processTimeTimer**：timer触发时，比如窗口结束触发计算就是使用timer实现（eventTimeTimer由watermark消息触发，所以包含在Task线程中）
> - **Async IO**：异步IO在结果返回时
>   上面这些逻辑需要与Task线程的处理逻辑互斥，否则就可能会有并发问题，造成数据错乱。最简单的解决办法就是-加锁，在Flink-1.10以前也是这么做的。

> *如果能将互斥的操作全部放在同一个线程中执行，是不是就不需要加锁了，天然串行执行。
> 所以在1.10版本中引入了Mailbox机制，外层抽象为类型`Executor`的接口，所有的互斥操作全部作为`Runnable`提交到`MailboxExecutor`，构建为`Mail`后加入`Mailbox`队列，`MailboxProcessor`在Task-Thread中运行，主要分为3种情况
>
> - 有特殊事件，即Mailbox不为空，优先处理完特殊事件。主要有checkpoint、timer
> - 无特殊事件且有network.buffer不为空，执行processInput处理数据
> - 无事可做，执行await等待唤醒

<img src="C:\Users\龙星宇\Desktop\xue\flink_study\img\M.png" alt="M" style="zoom: 50%;" />



# Flink网络栈



## 内存管理

### TaskManager中内存的划分

```
JVM ,大部分对象是临时产生的，很快就被销毁，垃圾回收压力大   =>  Flink自己管理内存
```

1. 向YARN申请容器，根据参数配置容器
2. 容器启动后，启动TaskManager时进行Flink内部管理的内存划分，根据配置，预先将内存块申请好进行池化处理，第一步只进行内存切分的计算，第二步在启动Worker时进行内存的细化划分



### TaskManager进程启动参数

* 计算需要从容器总体内预留的内存，设置参数containerized.heap-cutoff-ratio，最小值受containerized.heap-cutoff-min控制，这块内存预留给jvm使用，是一个经验值，主要作用是因在JVM中有一些非堆的内存开销，预留内存大小，防止内存超用后被杀

###  NetworkBuffer  Pool



## 网络传输

Netty 

TCP    TM      -->   多路复用



### 非流控模型

反压  -->   有限的Network Buffer 、  TCP 的流控





### 流控模型        1 .5引入

 

### 流批一体的 shuffle 架构





# Flink Connector



## Kafka Connector

























