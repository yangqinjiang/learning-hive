package com.atguigu.hive;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * 自定义udf函数,需要继承GenericUDF类
 * 需求: 计算指定字符串的长度
 */
public class MyStringLength extends GenericUDF {
    /**
     *
     * @param objectInspectors 输入参数类型的鉴别器对象
     * @return 返回值类型的鉴别器对象
     * @throws UDFArgumentException
     */
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        //判断输入参数的个数
        if(objectInspectors.length != 1){
            throw new UDFArgumentException("Input Args Length Error!!!");
        }
        //判断输入参数的类型
        if(!objectInspectors[0].getCategory().equals(ObjectInspector.Category.PRIMITIVE)){
            throw  new UDFArgumentTypeException(0,"Input Args Type Error!!!");
        }
        //函数本身返回值为int, 需要返回int类型的鉴别器对象

        return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
    }

    /**
     * 函数的逻辑处理
     * @param deferredObjects 输入的参数
     * @return 返回值
     * @throws HiveException
     */
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        if(deferredObjects[0].get() == null){
            return 0;
        }
        return deferredObjects[0].get().toString().length();
    }

    public String getDisplayString(String[] strings) {
        return "MyStringLength UDF DisplayString";
    }
}
