import java.util.Random;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;


public class ExpModelOrder {

    public static void main(String[] args) throws IoTDBConnectionException, StatementExecutionException {

        long startTime, endTime;
        String[] datasetList = {"zhongchuan", "zhongche", "wanhua", "PAMAP2", "stock", "gas"};
        String[] pList = {"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"};

        for (int index = 0; index < 6; index++) {
            String curDataset = datasetList[index];

            Session session = new Session("localhost", 6667, "root", "root");
            session.open();
            SessionDataSet result = null;

            int iterNum = 10;
            LogWriter lw = new LogWriter("./result/ExpModelOrder/UDF" + curDataset + "_res.dat");
            lw.open();
            lw.log("Order\tTimeCost" + "\n");
            for (int pIndex = 0; pIndex < pList.length; pIndex++) {
                String curP = pList[pIndex];
                StringBuilder str = new StringBuilder();
                startTime = System.currentTimeMillis();
                for (int i = 0; i < iterNum; i++) {
                    String sql = "select ar(s0, " +
                            "'p'='" + curP + "') " +
                            "from root." + curDataset + ".d0 ";
                    result = session.executeQueryStatement(sql);
                }

                endTime = System.currentTimeMillis();
                str.append(curP + "\t");
                str.append(String.format("%.4f", (endTime - startTime) / 1000.0 / iterNum) + "\n");
                lw.log(str);
            }
            lw.close();
            session.close();

            session = new Session("localhost", 6667, "root", "root");
            session.open();

            iterNum = 30;
            lw = new LogWriter("./result/ExpModelOrder/TsFile/" + curDataset + "_res.dat");
            lw.open();
            lw.log("Order\tTimeCost" + "\n");
            for (int pIndex = 0; pIndex < pList.length; pIndex++) {
                String curP = pList[pIndex];
                StringBuilder str = new StringBuilder();
                startTime = System.currentTimeMillis();
                for (int i = 0; i < iterNum; i++) {
                    String sql = "select ar(s0, " +
                            "'p'='" + curP + "') " +
                            "from root." + curDataset + ".d0 ";
                    result = session.executeQueryStatement(sql);
                }

                endTime = System.currentTimeMillis();
                str.append(curP + "\t");
                str.append(String.format("%.4f", (endTime - startTime) / 1000.0 / iterNum) + "\n");
                lw.log(str);
            }
            lw.close();
            session.close();
        }
    }
}
