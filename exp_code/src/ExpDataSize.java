import java.util.Random;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;

public class ExpDataSize {

    public static void main(String[] args) throws IoTDBConnectionException, StatementExecutionException {
        
        long startTime, endTime;
        String[] datasetList = {"zhongchuan", "zhongche", "wanhua", "PAMAP2", "stock", "gas"};
        String[] pList = {"8", "2", "7", "7", "2", "4"};
        int[] totalSizeList = {1327480, 1031976, 1054080, 374963, 1511298, 928990};
        int[] sizeList = null;

        for (int index = 0; index < 6; index++) {
            Session session = new Session("localhost", 6667, "root", "root");
            session.open();
            SessionDataSet result = null;
            
            String curDataset = datasetList[index];
            String curP = pList[index];
            int totalSize = totalSizeList[index];

            int iterNum = 10;
            String[] outputSizeList = new String[10];
            if (index == 0 || index == 1 || index == 2) {
                sizeList = new int[]{100000, 200000, 300000, 400000, 500000, 600000, 700000, 800000, 900000, 1000000};
            }
            else if (index == 3) {
                sizeList = new int[]{35000, 70000, 110000, 150000, 190000, 220000, 260000, 300000, 340000, 370000};
            }
            else if (index == 4) {
                sizeList = new int[]{150000, 300000, 450000, 600000, 750000, 900000, 1050000, 1200000, 1350000, 1500000};
            }
            else if (index == 5) {
                sizeList = new int[]{90000, 180000, 270000, 360000, 450000, 540000, 630000, 720000, 810000, 900000};
            }
            for (int  index_size = 0; index_size < 10; index_size++) {
                int size = sizeList[index_size];
                outputSizeList[index_size] = size / 1000 + "k";
            }

            LogWriter lw = new LogWriter("./result/ExpDataSize/UDF/" + curDataset + "_res.dat");
            lw.open();
            lw.log("Size\tTimeCost" + "\n");
            Random random = new Random();
            for (int index_size = 0; index_size < 10; index_size ++) {
                int size = sizeList[index_size];
                StringBuilder str = new StringBuilder();
                startTime = System.currentTimeMillis();
                for (int i = 0; i < iterNum; i++) {
                    long minTime = random.nextInt(totalSize - size) * 1000L;
                    long maxTime = minTime + size * 1000L;
                    String sql = "select ar_udf(s0, " +
                            "'p'='" + curP + "') " +
                            "from root." + curDataset + ".d0 " +
                            "where time>=" + minTime + " and time<" + maxTime;
                    result = session.executeQueryStatement(sql);
                }

                endTime = System.currentTimeMillis();
                str.append(outputSizeList[index_size] + "\t");
                str.append(String.format("%.4f", (endTime - startTime) / 1000.0 / iterNum) + "\n");
                lw.log(str);
            }
            lw.close();
            session.close();

            session = new Session("localhost", 6667, "root", "root");
            session.open();

            iterNum = 30;
            lw = new LogWriter("./result/ExpDataSize/TsFile/" + curDataset + "_res.dat");
            lw.open();
            lw.log("Size\tTimeCost" + "\n");
            random = new Random();
            for (int index_size = 0; index_size < 10; index_size ++) {
                int size = sizeList[index_size];
                StringBuilder str = new StringBuilder();
                startTime = System.currentTimeMillis();
                for (int i = 0; i < iterNum; i++) {
                    long minTime = random.nextInt(totalSize - size) * 1000L;
                    long maxTime = minTime + size * 1000L;
                    String sql = "select ar(s0, " +
                            "'p'='" + curP + "') " +
                            "from root." + curDataset + ".d0 " +
                            "where time>=" + minTime + " and time<" + maxTime;
                    result = session.executeQueryStatement(sql);
                }

                endTime = System.currentTimeMillis();
                str.append(outputSizeList[index_size] + "\t");
                str.append(String.format("%.4f", (endTime - startTime) / 1000.0 / iterNum) + "\n");
                lw.log(str);
            }
            lw.close();
            session.close();
        }
    }
}
