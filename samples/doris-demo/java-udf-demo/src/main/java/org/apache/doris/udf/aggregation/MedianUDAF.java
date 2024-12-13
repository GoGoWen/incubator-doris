package org.apache.doris.udf.aggregation;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.logging.Logger;

/*UDAF 计算中位数*/
public class MedianUDAF {
    Logger log = Logger.getLogger("MedianUDAF");

    //状态存储
    public static class State {
        //返回结果的精度
        int scale = 0;
        //是否是某一个 tablet 下的某个聚合条件下的数据第一次执行 add 方法
        boolean isFirst = true;
        //数据存储
        public StringBuilder stringBuilder;
    }

    //状态初始化
    public State create() {
        State state = new State();
        //根据每个 tablet 下的聚合条件需要聚合的数据量大小，预先初始化，增加性能
        state.stringBuilder = new StringBuilder(1000);
        return state;
    }


    //处理执行单位处理各自 tablet 下的各自聚合条件下的每个数据
    public void add(State state, Double val, int scale) {
        try {
            if (val != null && state.isFirst) {
                state.stringBuilder.append(scale).append(",").append(val).append(",");
                state.isFirst = false;
            } else if (val != null) {
                state.stringBuilder.append(val).append(",");
            }
        } catch (Exception e) {
            //如果不能保证一定不会异常，建议每个方法都最大化捕获异常，因为目前不支持处理 java 抛出的异常
            log.info("获取数据异常：" + e.getMessage());
        }
    }

    //处理数据完需要输出等待聚合
    public void serialize(State state, DataOutputStream out) {
        try {
            //目前暂时只提供 DataOutputStream，如果需要序列化对象可以考虑拼接字符串，转换 json，序列化成字节数组等方式
            //如果要序列化 State 对象，可能需要自己将 State 内部类实现序列化接口
            //最终都是要通过 DataOutputStream 传输
            out.writeUTF(state.stringBuilder.toString());
        } catch (Exception e) {
            log.info("序列化异常：" + e.getMessage());
        }
    }

    //获取处理数据执行单位输出的数据
    public void deserialize(State state, DataInputStream in) {
        try {
            String string = in.readUTF();
            state.scale = Integer.parseInt(String.valueOf(string.charAt(0)));
            StringBuilder stringBuilder = new StringBuilder(string.substring(2));
            state.stringBuilder = stringBuilder;
        } catch (Exception e) {
            log.info("反序列化异常：" + e.getMessage());
        }
    }

    //聚合执行单位按照聚合条件合并某一个键下数据的处理结果 ,每个键第一次合并时，state1 参数是初始化的实例
    public void merge(State state1, State state2) {
        try {
            state1.scale = state2.scale;
            state1.stringBuilder.append(state2.stringBuilder.toString());
        } catch (Exception e) {
            log.info("合并结果异常：" + e.getMessage());
        }
    }

    //对每个键合并后的数据进行并输出最终结果
    public Double getValue(State state) {
        try {
            String[] strings = state.stringBuilder.toString().split(",");
            double[] doubles = new double[strings.length + 1];
            doubles = Arrays.stream(strings).mapToDouble(Double::parseDouble).toArray();

            Arrays.sort(doubles);
            double n = doubles.length - 1;
            double index = n * 0.5;

            int low = (int) Math.floor(index);
            int high = (int) Math.ceil(index);

            double value = low == high ? (doubles[low] + doubles[high]) * 0.5 : doubles[high];

            BigDecimal decimal = new BigDecimal(value);
            return decimal.setScale(state.scale, BigDecimal.ROUND_HALF_UP).doubleValue();
        } catch (Exception e) {
            log.info("计算异常：" + e.getMessage());
        }
        return 0.0;
    }

    //每个执行单位执行完都会执行
    public void destroy(State state) {
    }

}
