package Tde.ExercicioFour;
import org.apache.hadoop.io.WritableComparable;

import java.util.Objects;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
public class ExerciciofourValueWritable implements WritableComparable<ExerciciofourValueWritable> {
    private int reapt;
    private double preco;
    public ExerciciofourValueWritable() {
    }
    public ExerciciofourValueWritable(int reapt, double preco) {
        this.reapt = reapt;
        this.preco = preco;
    }

    public int getReapt() {
        return reapt;
    }

    public void setReapt(int reapt) {
        this.reapt = reapt;
    }

    public double getPreco() {
        return preco;
    }

    public void setPreco(double preco) {
        this.preco = preco;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ExerciciofourValueWritable)) return false;
        ExerciciofourValueWritable that = (ExerciciofourValueWritable) o;
        return reapt == that.reapt && Double.compare(that.preco, preco) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(reapt, preco);
    }
    @Override
    public int compareTo(ExerciciofourValueWritable o) {
        return Integer.compare(this.hashCode(),o.hashCode());
    }


    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(reapt);
        out.writeDouble(preco);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        reapt = in.readInt();
        preco = in.readDouble();
    }



}


