package Tde.ExercicioFour;
import org.apache.hadoop.io.WritableComparable;

import java.util.Objects;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
public class ExerciciofourKeyWritable implements WritableComparable<ExerciciofourKeyWritable> {

    private String unit;
    private String year;

    private String category;

    public ExerciciofourKeyWritable(String unit, String year, String category) {
        this.unit = unit;
        this.year = year;
        this.category = category;
    }

    public String getUnit() {
        return unit;
    }

    public void setUnit(String unit) {
        this.unit = unit;
    }

    public ExerciciofourKeyWritable() {
    }

    public String getYear() {
        return year;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public void setYear(String year) {
        this.year = year;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ExerciciofourKeyWritable)) return false;
        ExerciciofourKeyWritable that = (ExerciciofourKeyWritable) o;
        return Objects.equals(unit, that.unit) && Objects.equals(year, that.year) && Objects.equals(category, that.category);
    }

    @Override
    public int hashCode() {
        return Objects.hash(unit, year, category);
    }

    @Override
    public int compareTo(ExerciciofourKeyWritable o) {
        return Integer.compare(this.hashCode(),o.hashCode());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(unit);
        out.writeUTF(year);
        out.writeUTF(category);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        unit = in.readUTF();
        year = in.readUTF();
        category = in.readUTF();

    }
}



