package Tde.ExercicioFive;
import org.apache.hadoop.io.WritableComparable;

import java.util.Objects;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
public class ExercicioFiveValuesWritable implements WritableComparable<ExercicioFiveValuesWritable> {
      private double soma;
      private int count;
      private double max;
      private double min;

      public ExercicioFiveValuesWritable() {
      }

      public ExercicioFiveValuesWritable(double soma, int count, double max, double min) {
            this.soma = soma;
            this.count = count;
            this.max = max;
            this.min = min;
      }

      public double getSoma() {
            return soma;
      }

      public void setSoma(double soma) {
            this.soma = soma;
      }

      public int getCount() {
            return count;
      }

      public void setCount(int count) {
            this.count = count;
      }

      public double getMax() {
            return max;
      }

      public void setMax(double max) {
            this.max = max;
      }

      public double getMin() {
            return min;
      }

      public void setMin(double min) {
            this.min = min;
      }

      @Override
      public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof ExercicioFiveValuesWritable)) return false;
            ExercicioFiveValuesWritable that = (ExercicioFiveValuesWritable) o;
            return Double.compare(that.soma, soma) == 0 && count == that.count && Double.compare(that.max, max) == 0 && Double.compare(that.min, min) == 0;
      }

      @Override
      public int hashCode() {
            return Objects.hash(soma, count, max, min);
      }

      @Override
      public void write(DataOutput out) throws IOException {
            out.writeDouble(soma);
            out.writeInt(count);
            out.writeDouble(max);
            out.writeDouble(min);
      }

      @Override
      public void readFields(DataInput in) throws IOException {
            soma = in.readDouble();
            count = in.readInt();
            max = in.readDouble();
            min = in.readDouble();
      }

      @Override
      public int compareTo(ExercicioFiveValuesWritable o) {
            return Integer.compare(this.hashCode(),o.hashCode());
      }
}
