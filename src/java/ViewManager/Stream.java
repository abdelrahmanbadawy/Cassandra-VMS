package ViewManager;

import com.datastax.driver.core.Row;

public class Stream {

	private Row innerJoinAggGroupByOldRow;
	private Row innerJoinAggGroupByUpdatedOldRow;
	private Row innerJoinAggGroupByNewRow;
	private Row innerJoinAggGroupByDeleteRow;
	private Row leftOrRightJoinAggGroupByOldRow;
	private Row leftOrRightJoinAggGroupByUpdatedOldRow;
	private Row leftOrRightJoinAggGroupByNewRow;
	private Row leftOrRightJoinAggGroupByDeleteRow;

	public Stream() {
		setInnerJoinAggGroupByOldRow(null);
		setInnerJoinAggGroupByUpdatedOldRow(null);
		setInnerJoinAggGroupByNewRow(null);
		setLeftOrRightJoinAggGroupByNewRow(null);
		setLeftOrRightJoinAggGroupByOldRow(null);
		setLeftOrRightJoinAggGroupByUpdatedOldRow(null);
		setInnerJoinAggGroupByDeleteOldRow(null);
		setLeftOrRightJoinAggGroupByDeleteRow(null);
	}

	public Row getInnerJoinAggGroupByOldRow() {
		return innerJoinAggGroupByOldRow;
	}

	public void setInnerJoinAggGroupByOldRow(Row joinAggGroupByOldRow) {
		this.innerJoinAggGroupByOldRow = joinAggGroupByOldRow;
	}

	public Row getInnerJoinAggGroupByUpdatedOldRow() {
		return innerJoinAggGroupByUpdatedOldRow;
	}

	public void setInnerJoinAggGroupByUpdatedOldRow(
			Row joinAggGroupByUpdatedOldRow) {
		this.innerJoinAggGroupByUpdatedOldRow = joinAggGroupByUpdatedOldRow;
	}

	public Row getInnerJoinAggGroupByNewRow() {
		return innerJoinAggGroupByNewRow;
	}

	public void setInnerJoinAggGroupByNewRow(Row joinAggGroupByNewRow) {
		this.innerJoinAggGroupByNewRow = joinAggGroupByNewRow;
	}

	public Row getLeftOrRightJoinAggGroupByOldRow() {
		return leftOrRightJoinAggGroupByOldRow;
	}

	public void setLeftOrRightJoinAggGroupByOldRow(
			Row leftOrRightJoinAggGroupByOldRow) {
		this.leftOrRightJoinAggGroupByOldRow = leftOrRightJoinAggGroupByOldRow;
	}

	public Row getLeftOrRightJoinAggGroupByNewRow() {
		return leftOrRightJoinAggGroupByNewRow;
	}

	public void setLeftOrRightJoinAggGroupByNewRow(
			Row leftOrRightJoinAggGroupByNewRow) {
		this.leftOrRightJoinAggGroupByNewRow = leftOrRightJoinAggGroupByNewRow;
	}

	public Row getLeftOrRightJoinAggGroupByUpdatedOldRow() {
		return leftOrRightJoinAggGroupByUpdatedOldRow;
	}

	public void setLeftOrRightJoinAggGroupByUpdatedOldRow(
			Row leftOrRightJoinAggGroupByUpdatedOldRow) {
		this.leftOrRightJoinAggGroupByUpdatedOldRow = leftOrRightJoinAggGroupByUpdatedOldRow;
	}

	public void resetJoinAggGroupByUpRows(){
		setInnerJoinAggGroupByOldRow(null);
		setInnerJoinAggGroupByUpdatedOldRow(null);
		setInnerJoinAggGroupByNewRow(null);
		setInnerJoinAggGroupByDeleteOldRow(null);
		setLeftOrRightJoinAggGroupByNewRow(null);
		setLeftOrRightJoinAggGroupByOldRow(null);
		setLeftOrRightJoinAggGroupByUpdatedOldRow(null);
		setLeftOrRightJoinAggGroupByDeleteRow(null);
	}

	public Row getInnerJoinAggGroupByDeleteOldRow() {
		return innerJoinAggGroupByDeleteRow;
	}

	public void setInnerJoinAggGroupByDeleteOldRow(
			Row innerJoinAggGroupByDeleteRow) {
		this.innerJoinAggGroupByDeleteRow = innerJoinAggGroupByDeleteRow;
	}

	public Row getLeftOrRightJoinAggGroupByDeleteRow() {
		return leftOrRightJoinAggGroupByDeleteRow;
	}

	public void setLeftOrRightJoinAggGroupByDeleteRow(
			Row leftOrRightJoinAggGroupByDeleteRow) {
		this.leftOrRightJoinAggGroupByDeleteRow = leftOrRightJoinAggGroupByDeleteRow;
	}


}
