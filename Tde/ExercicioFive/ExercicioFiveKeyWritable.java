package Tde.ExercicioFive;

import org.apache.hadoop.io.WritableComparable;

import java.util.Objects;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ExercicioFiveKeyWritable implements WritableComparable<ExercicioFiveKeyWritable> {
    private String year;
    private  String unit;


    public ExercicioFiveKeyWritable() {
    }
    public ExercicioFiveKeyWritable(String year, String unit) {
        this.year = year;
        this.unit = unit;

    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public String getUnit() {
        return unit;
    }

    public void setUnit( String unit) {
        this.unit = unit;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ExercicioFiveKeyWritable)) return false;
        ExercicioFiveKeyWritable that = (ExercicioFiveKeyWritable) o;
        return Objects.equals(year, that.year) && Objects.equals(unit, that.unit);
    }

    @Override
    public int hashCode() {
        return Objects.hash(year, unit);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(year);
        dataOutput.writeUTF(unit);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        //leitura de dados precisa seguir a ordem do write
        year=dataInput.readUTF();
        unit=dataInput.readUTF();


    }




    @Override
    public int compareTo(ExercicioFiveKeyWritable o) {
        return Integer.compare(this.hashCode(),o.hashCode());
    }
}

