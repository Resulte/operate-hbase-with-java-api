package cn.edu.bupt;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Main {

    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.rootdir","hdfs://localhost:9000/hbase");
        conf.set("hbase.cluster.distributed","true");

        Connection connection = ConnectionFactory.createConnection(conf);

        HBaseAdmin admin = (HBaseAdmin)connection.getAdmin();

        // 创建Orders表 & 创建Order Detail和Transaction两个列族
        TableDescriptorBuilder tdb = TableDescriptorBuilder.newBuilder(TableName.valueOf("Orders"));
        List<ColumnFamilyDescriptor> listColumns = new ArrayList<>();
        ColumnFamilyDescriptor cfd_info = ColumnFamilyDescriptorBuilder.newBuilder("Order Detail".getBytes()).build();
        ColumnFamilyDescriptor cfd_res = ColumnFamilyDescriptorBuilder.newBuilder("Transaction".getBytes()).build();
        listColumns.add(cfd_info);
        listColumns.add(cfd_res);
        tdb.setColumnFamilies(listColumns);
        TableDescriptor td= tdb.build();
        admin.createTable(td);

        // 表中加入数据
        HTable table = (HTable) connection.getTable(TableName.valueOf("Orders"));
        Put put=new Put("000001".getBytes());
        put.addColumn("Order Detail".getBytes(),"consumerId".getBytes(),"41341".getBytes());
        put.addColumn("Order Detail".getBytes(),"itemId".getBytes(),"1057499".getBytes());
        put.addColumn("Order Detail".getBytes(),"itemCategory".getBytes(),"2".getBytes());
        put.addColumn("Order Detail".getBytes(),"amount".getBytes(),"1".getBytes());
        put.addColumn("Order Detail".getBytes(),"money".getBytes(),"462.8".getBytes());
        put.addColumn("Transaction".getBytes(),"createTime".getBytes(),"2020-4-16 9:21:09".getBytes());
        put.addColumn("Transaction".getBytes(),"paymentTime".getBytes(),"2020-4-16 10:14:47".getBytes());
        put.addColumn("Transaction".getBytes(),"deliveryTime".getBytes(),"2020-7-9 2:12:23".getBytes());
        put.addColumn("Transaction".getBytes(),"CompleteTime".getBytes(),"2020-7-28 12:10:40".getBytes());
        table.put(put);

        put=new Put("000002".getBytes());
        put.addColumn("Order Detail".getBytes(),"consumerId".getBytes(),"32805".getBytes());
        put.addColumn("Order Detail".getBytes(),"itemId".getBytes(),"9203020".getBytes());
        put.addColumn("Order Detail".getBytes(),"itemCategory".getBytes(),"4".getBytes());
        put.addColumn("Order Detail".getBytes(),"amount".getBytes(),"0".getBytes());
        put.addColumn("Order Detail".getBytes(),"money".getBytes(),"760.3".getBytes());
        put.addColumn("Transaction".getBytes(),"createTime".getBytes(),"2020-3-17 0:17:19".getBytes());
        put.addColumn("Transaction".getBytes(),"paymentTime".getBytes(),"2020-3-17 1:15:04".getBytes());
        put.addColumn("Transaction".getBytes(),"deliveryTime".getBytes(),"2020-7-4 21:13:27".getBytes());
        put.addColumn("Transaction".getBytes(),"CompleteTime".getBytes(),"2020-9-22 6:58:44".getBytes());
        table.put(put);

        put=new Put("000003".getBytes());
        put.addColumn("Order Detail".getBytes(),"consumerId".getBytes(),"66772".getBytes());
        put.addColumn("Order Detail".getBytes(),"itemId".getBytes(),"6330669".getBytes());
        put.addColumn("Order Detail".getBytes(),"itemCategory".getBytes(),"5".getBytes());
        put.addColumn("Order Detail".getBytes(),"amount".getBytes(),"9".getBytes());
        put.addColumn("Order Detail".getBytes(),"money".getBytes(),"97".getBytes());
        put.addColumn("Transaction".getBytes(),"createTime".getBytes(),"2020-4-6 11:34:17".getBytes());
        put.addColumn("Transaction".getBytes(),"paymentTime".getBytes(),"2020-4-6 11:37:04".getBytes());
        put.addColumn("Transaction".getBytes(),"deliveryTime".getBytes(),"2020-5-10 10:18:01".getBytes());
        put.addColumn("Transaction".getBytes(),"CompleteTime".getBytes(),"2020-7-1 8:27:19".getBytes());
        table.put(put);

        put=new Put("000004".getBytes());
        put.addColumn("Order Detail".getBytes(),"consumerId".getBytes(),"59086".getBytes());
        put.addColumn("Order Detail".getBytes(),"itemId".getBytes(),"5544997".getBytes());
        put.addColumn("Order Detail".getBytes(),"itemCategory".getBytes(),"4".getBytes());
        put.addColumn("Order Detail".getBytes(),"amount".getBytes(),"4".getBytes());
        put.addColumn("Order Detail".getBytes(),"money".getBytes(),"550.1".getBytes());
        put.addColumn("Transaction".getBytes(),"createTime".getBytes(),"2020-3-26 22:29:44".getBytes());
        put.addColumn("Transaction".getBytes(),"paymentTime".getBytes(),"2020-3-26 22:51:19".getBytes());
        put.addColumn("Transaction".getBytes(),"deliveryTime".getBytes(),"2020-7-5 10:09:26".getBytes());
        put.addColumn("Transaction".getBytes(),"CompleteTime".getBytes(),"2020-9-13 15:47:28".getBytes());
        table.put(put);

        put=new Put("000005".getBytes());
        put.addColumn("Order Detail".getBytes(),"consumerId".getBytes(),"94847".getBytes());
        put.addColumn("Order Detail".getBytes(),"itemId".getBytes(),"5377359".getBytes());
        put.addColumn("Order Detail".getBytes(),"itemCategory".getBytes(),"1".getBytes());
        put.addColumn("Order Detail".getBytes(),"amount".getBytes(),"6".getBytes());
        put.addColumn("Order Detail".getBytes(),"money".getBytes(),"87.4".getBytes());
        put.addColumn("Transaction".getBytes(),"createTime".getBytes(),"2020-4-23 8:43:27".getBytes());
        put.addColumn("Transaction".getBytes(),"paymentTime".getBytes(),"2020-4-23 9:11:33".getBytes());
        put.addColumn("Transaction".getBytes(),"deliveryTime".getBytes(),"2020-4-29 11:31:01".getBytes());
        put.addColumn("Transaction".getBytes(),"CompleteTime".getBytes(),"2020-5-15 3:24:54".getBytes());
        table.put(put);

        put=new Put("000006".getBytes());
        put.addColumn("Order Detail".getBytes(),"consumerId".getBytes(),"92140".getBytes());
        put.addColumn("Order Detail".getBytes(),"itemId".getBytes(),"8739695".getBytes());
        put.addColumn("Order Detail".getBytes(),"itemCategory".getBytes(),"5".getBytes());
        put.addColumn("Order Detail".getBytes(),"amount".getBytes(),"9".getBytes());
        put.addColumn("Order Detail".getBytes(),"money".getBytes(),"819".getBytes());
        put.addColumn("Transaction".getBytes(),"createTime".getBytes(),"2020-2-9 1:03:14".getBytes());
        put.addColumn("Transaction".getBytes(),"paymentTime".getBytes(),"2020-2-9 1:13:04".getBytes());
        put.addColumn("Transaction".getBytes(),"deliveryTime".getBytes(),"2020-5-3 0:06:12".getBytes());
        put.addColumn("Transaction".getBytes(),"CompleteTime".getBytes(),"2020-8-19 23:38:05".getBytes());
        table.put(put);

        put=new Put("000007".getBytes());
        put.addColumn("Order Detail".getBytes(),"consumerId".getBytes(),"13851".getBytes());
        put.addColumn("Order Detail".getBytes(),"itemId".getBytes(),"9503980".getBytes());
        put.addColumn("Order Detail".getBytes(),"itemCategory".getBytes(),"9".getBytes());
        put.addColumn("Order Detail".getBytes(),"amount".getBytes(),"7".getBytes());
        put.addColumn("Order Detail".getBytes(),"money".getBytes(),"671.3".getBytes());
        put.addColumn("Transaction".getBytes(),"createTime".getBytes(),"2020-2-18 15:05:57".getBytes());
        put.addColumn("Transaction".getBytes(),"paymentTime".getBytes(),"2020-2-18 15:57:22".getBytes());
        put.addColumn("Transaction".getBytes(),"deliveryTime".getBytes(),"2020-5-8 9:39:56".getBytes());
        put.addColumn("Transaction".getBytes(),"CompleteTime".getBytes(),"2020-5-27 3:10:54".getBytes());
        table.put(put);

        put=new Put("000008".getBytes());
        put.addColumn("Order Detail".getBytes(),"consumerId".getBytes(),"42138".getBytes());
        put.addColumn("Order Detail".getBytes(),"itemId".getBytes(),"2682154".getBytes());
        put.addColumn("Order Detail".getBytes(),"itemCategory".getBytes(),"5".getBytes());
        put.addColumn("Order Detail".getBytes(),"amount".getBytes(),"3".getBytes());
        put.addColumn("Order Detail".getBytes(),"money".getBytes(),"11.6".getBytes());
        put.addColumn("Transaction".getBytes(),"createTime".getBytes(),"2020-2-28 6:54:33".getBytes());
        put.addColumn("Transaction".getBytes(),"paymentTime".getBytes(),"2020-2-28 7:18:10".getBytes());
        put.addColumn("Transaction".getBytes(),"deliveryTime".getBytes(),"2020-3-25 22:24:11".getBytes());
        put.addColumn("Transaction".getBytes(),"CompleteTime".getBytes(),"2020-6-27 10:04:31".getBytes());
        table.put(put);

        table.close();
        admin.close();
        connection.close();
    }
}
