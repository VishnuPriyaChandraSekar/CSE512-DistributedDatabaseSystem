package MapReduce_Join.Assignment4;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class Table implements Writable {
	public Text tablename=new Text();
	public Text columns= new Text();
	public Table(){
		
	}
	public Table(Text tablename, Text columns) {
		this.tablename.set(tablename);
		this.columns.set(columns);
	}

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		this.tablename.write(out);
		this.columns.write(out);
	}
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.tablename.readFields(in);
		this.columns.readFields(in);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((tablename == null) ? 0 : tablename.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Table other = (Table) obj;
		if (tablename == null) {
			if (other.tablename != null)
				return false;
		} else if (!tablename.equals(other.tablename))
			return false;
		return true;
	}

	
}
