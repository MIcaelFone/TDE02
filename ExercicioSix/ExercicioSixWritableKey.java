package Tde.ExercicioSix;
import org.apache.hadoop.io.WritableComparable;

import java.util.Objects;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
public class ExercicioSixWritableKey implements WritableComparable<ExercicioSixWritableKey> {
    private String country;
    private double maxprice;

    public ExercicioSixWritableKey(double maxprice, String country) {
        this.maxprice = maxprice;
        this.country = country;
    }
    public ExercicioSixWritableKey() {

    }

    public double getMaxprice() {
        return maxprice;
    }

    public void setMaxprice(double maxprice) {
        this.maxprice = maxprice;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ExercicioSixWritableKey)) return false;
        ExercicioSixWritableKey that = (ExercicioSixWritableKey) o;
        return Double.compare(that.maxprice, maxprice) == 0 && Objects.equals(country, that.country);
    }

    @Override
    public int hashCode() {
        return Objects.hash(country, maxprice);
    }



    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(maxprice);
        out.writeUTF(country);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        maxprice = in.readDouble();
        country = in.readUTF();

    }
    @Override
    public int compareTo(ExercicioSixWritableKey o) {
        return Integer.compare(this.hashCode(),o.hashCode());
    }
}
