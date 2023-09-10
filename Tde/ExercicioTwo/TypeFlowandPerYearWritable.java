package Tde.ExercicioTwo;

import org.apache.hadoop.io.WritableComparable;

import java.util.Objects;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TypeFlowandPerYearWritable implements WritableComparable<TypeFlowandPerYearWritable> {
    private String year;
    private String flow;


    public TypeFlowandPerYearWritable() {
    }
    public TypeFlowandPerYearWritable(String year, String flow) {
        this.year = year;
        this.flow = flow;

    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public String getFlow() {
        return flow;
    }

    public void setFlow(String flow) {
        this.flow = flow;
    }



    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TypeFlowandPerYearWritable)) return false;
        TypeFlowandPerYearWritable that = (TypeFlowandPerYearWritable) o;
        return year == that.year  && Objects.equals(flow, that.flow);
    }

    @Override
    public int hashCode() {
        return Objects.hash(year, flow);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(year);
        dataOutput.writeUTF(flow);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        //leitura de dados precisa seguir a ordem do write
        year=dataInput.readUTF();
        flow=dataInput.readUTF();


    }


    @Override
    public int compareTo(TypeFlowandPerYearWritable o) {
        return Integer.compare(this.hashCode(),o.hashCode());
    }
}
