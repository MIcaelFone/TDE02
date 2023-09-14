package Tde.ExercicioThree;
import org.apache.hadoop.io.WritableComparable;

import java.util.Objects;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
public class ReaptandSomaWritable implements WritableComparable<ReaptandSomaWritable> {
    private int reapt;
    private double preco;
    public ReaptandSomaWritable() {
    }
    public ReaptandSomaWritable(int reapt, double preco) {
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
        if (!(o instanceof ReaptandSomaWritable)) return false;
        ReaptandSomaWritable that = (ReaptandSomaWritable) o;
        return reapt == that.reapt && preco == that.preco;
    }

    @Override
    public int hashCode() {
        return Objects.hash(reapt, preco);
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

    @Override
     public int compareTo(ReaptandSomaWritable o) {
            return Integer.compare(this.hashCode(),o.hashCode());
        }
    }

