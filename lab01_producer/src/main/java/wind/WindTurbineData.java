package wind;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.NotNull;

public class WindTurbineData {
    @JsonProperty("wind_turbine_id")
    @NotNull
    public String windTurbineId;

    @JsonProperty("wind_park_id")
    @NotNull
    public String windParkId;

    @JsonProperty("current_power")
    @NotNull
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
