 package Tde.AvagereOfCommodityPerYear;
import org.apache.hadoop.io.WritableComparable;

import java.util.Objects;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
public class ReaptandSomaWritable implements WritableComparable<ReaptandSomaWritable> {
    private int reapt;
    private float custo;
    public ReaptandSomaWritable() {
    }
    public ReaptandSomaWritable(int reapt, long custo) {
        this.reapt = reapt;
        this.custo = custo;
    }

    public int getReapt() {
        return reapt;
    }

    public void setReapt(int reapt) {
        this.reapt = reapt;
    }

    public float getCusto() {
        return custo;
    }

    public void setCusto(Long custo) {
        this.custo = custo;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ReaptandSomaWritable)) return false;
        ReaptandSomaWritable that = (ReaptandSomaWritable) o;
        return reapt == that.reapt && custo == that.custo;
    }

    @Override
    public int hashCode() {
        return Objects.hash(reapt, custo);
    }



    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(reapt);
        out.writeFloat(custo);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        reapt = in.readInt();
        custo = in.readFloat();
    }

    @Override
     public int compareTo(ReaptandSomaWritable o) {
            return Integer.compare(this.hashCode(),o.hashCode());
        }
    }


