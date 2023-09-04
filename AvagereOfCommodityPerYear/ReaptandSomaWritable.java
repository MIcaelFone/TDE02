package Tde.AvagereOfCommodityPerYear;
import org.apache.hadoop.io.WritableComparable;
import java.util.Objects;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
public class ReaptandSomaWritable implements WritableComparable<ReaptandSomaWritable>{
    private int reapt;
    private String soma;

    public ReaptandSomaWritable() {
    }
    public ReaptandSomaWritable(int reapt, String soma) {
        this.reapt = reapt;
        this.soma = soma;
    }

    public int getReapt() {
        return reapt;
    }


    public void setReapt(int reapt) {
        this.reapt = reapt;
    }

    public float getSoma() {
        return soma;
    }

    public void setSoma(float soma) {
        this.soma = soma;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ReaptandSomaWritable)) return false;
        ReaptandSomaWritable that = (ReaptandSomaWritable) o;
        return reapt == that.reapt && Float.compare(that.soma, soma) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(reapt, soma);
    }

    @Override
    public int compareTo(ReaptandSomaWritable o) {
        return Integer.compare(this.hashCode(),o.hashCode());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(reapt);
        dataOutput.writeFloat(soma);


    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        reapt = dataInput.readInt();
        soma = dataInput.readFloat();

    }
}
