package com.flink.demo.chapter12;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;

public class MyCountMapFunction implements MapFunction<String, Long>, CheckpointedFunction {
    private Long count = 0L;
    private ListState<Long> state;
    @Override
    public Long map(String value) throws Exception {
        return ++count;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        System.out.println("snapshotState...");
        // 2.1 清空算子状态
        state.clear();
        // 2.2 将 本地变量 添加到 算子状态 中
        state.add(count);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        System.out.println("initializeState...");
        // 3.1 从 上下文 初始化 算子状态
        state = context
                .getOperatorStateStore()
                .getListState(new ListStateDescriptor<Long>("state", Types.LONG));
                //.getUnionListState()
        // 3.2 从 算子状态中 把数据 拷贝到 本地变量
        if (context.isRestored()) {
            for (Long c : state.get()) {
                count += c;
            }
        }
    }
}

/**
 * 与 ListState 类似，联合列表状态也会将状态表示为一个列表。它与常规列表状态的区别
 * 在于，算子并行度进行缩放调整时对于状态的分配方式不同。
 * UnionListState的重点就在于“联合”（union）。在并行度调整时，常规列表状态是轮询分
 * 配状态项，而联合列表状态的算子则会直接广播状态的完整列表。这样，并行度缩放之后的
 * 并行子任务就获取到了联合后完整的“大列表”，可以自行选择要使用的状态项和要丢弃的状
 * 态项。这种分配也叫作“联合重组”（union redistribution）。如果列表中状态项数量太多，为
 * 资源和效率考虑一般不建议使用联合重组的方式。
 *
 */