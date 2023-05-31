import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;


public class ExpOverlapLength {

    public static void main(String[] args) throws IoTDBConnectionException, StatementExecutionException {
        long startTime, endTime;
        String[] datasetList = {"zhongchuan", "zhongche", "wanhua", "PAMAP2", "stock", "gas"};
        String[] pList = {"8", "2", "7", "7", "2", "4"};
        int[] totalSizeList = {1327480, 1031976, 1054080, 374963, 1511298, 928990};

        String overlap_length = "50";

        for (int index = 0; index < 6; index ++) {
            String curDataset = datasetList[index];
            String curP = pList[index];

            Session session = new Session("localhost", 6667, "root", "root");
            session.open();
            SessionDataSet result = null;

            int iterNum = 10;
            LogWriter lw = new LogWriter("./result/ExpOverlapLength/UDF/" + curDataset + "_res.dat");
            lw.open();
            lw.log("Length\tTimeCost" + "\n");
            StringBuilder str = new StringBuilder();
            startTime = System.currentTimeMillis();
            for (int i = 0; i < iterNum; i++) {
                String sql = "select ar_udf(s0, " +
                        "'p'='" + curP + "') " +
                        "from root." + curDataset + ".d0 ";
                result = session.executeQueryStatement(sql);
            }
            endTime = System.currentTimeMillis();
            str.append(overlap_length + "\t");
            str.append(String.format("%.4f", (endTime - startTime) / 1000.0 / iterNum) + "\n");
            lw.log(str);
            lw.close();
            session.close();

            session = new Session("localhost", 6667, "root", "root");
            session.open();
            iterNum = 30;
            lw = new LogWriter("./result/ExpOverlapLength/TsFile/" + curDataset + "_res.dat");
            lw.open();
            lw.log("Length\tTimeCost" + "\n");

            str = new StringBuilder();
            startTime = System.currentTimeMillis();
            for (int i = 0; i < iterNum; i++) {
                String sql = "select ar(s0, " +
                        "'p'='" + curP + "', " +
                        "'level'='chunk') " +
                        "from root." + curDataset + ".d0 ";
                result = session.executeQueryStatement(sql);
            }

            endTime = System.currentTimeMillis();
            str.append(overlap_length + "\t");
            str.append(String.format("%.4f", (endTime - startTime) / 1000.0 / iterNum) + "\n");
            lw.log(str);
            lw.close();
            session.close();
        }
    }
}
