package wind;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

public class WindTurbineData {
    @JsonProperty
    public String windTurbineId;
    @JsonProperty
    public String windParkId;
    @JsonProperty
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