package wind;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

public class WindTurbineData {
    @JsonProperty("wind_turbine_id")
    public String windTurbineId;

    @JsonProperty("wind_park_id")
    public String windParkId;

    @JsonProperty("current_power")
    public double currentPower;

    // Jackson needs an empty constructor
    private WindTurbineData() {

    }

    public WindTurbineData(String windTurbineId, String windParkId, double currentPower) {
        this.windTurbineId = windTurbineId;
        this.windParkId = windParkId;
        this.currentPower = currentPower;
    }

    @Override
    public String toString() {
        return "WindTurbineData{" +
                "windTurbineId='" + windTurbineId + '\'' +
                ", windParkId='" + windParkId + '\'' +
                ", currentPower=" + currentPower +
                '}';
    }
}
