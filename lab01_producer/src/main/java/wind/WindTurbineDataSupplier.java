package wind;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.Supplier;

public class WindTurbineDataSupplier implements Supplier<WindTurbineData> {

    private final Random rand = new Random();
    private final double msgsPerSec;
    List<TurbineProperties> turbines = new ArrayList<>();
    private int msgCount = 0;

    public WindTurbineDataSupplier(int numTurbines, double msgsPerSec) {
        this.msgsPerSec = msgsPerSec;
        for (int i = 0; i < numTurbines; i++) {
            turbines.add(new TurbineProperties(rand.nextDouble(5000, 1.5 * 1000 * 1000),
                    "Turbine" + i,
                    "Windpark" + (i % 5)));
        }
    }

    @Override
    public WindTurbineData get() {

        TurbineProperties turbine = turbines.get(rand.nextInt(turbines.size()));

        WindTurbineData windTurbineData = new WindTurbineData(turbine.id, turbine.parkId,
                rand.nextDouble(0, turbine.maxPower));
        if (msgsPerSec != -1) {
            try {
                Thread.sleep((long) ((1 / msgsPerSec) * 1000));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            msgCount = 0;
        }
        msgCount++;
        return windTurbineData;
    }

    public record TurbineProperties(
            double maxPower,
            String id,
            String parkId) {
    }
}
