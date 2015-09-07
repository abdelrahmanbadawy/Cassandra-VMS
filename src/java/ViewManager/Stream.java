package ViewManager;

import com.datastax.driver.core.Row;

public class Stream {

	//JoinAggGroupBy
	private Row innerJoinAggGroupByOldRow;
	private Row innerJoinAggGroupByUpdatedOldRow;
	private Row innerJoinAggGroupByNewRow;
	private Row innerJoinAggGroupByDeleteRow;
	private Row leftOrRightJoinAggGroupByOldRow;
	private Row leftOrRightJoinAggGroupByUpdatedOldRow;
	private Row leftOrRightJoinAggGroupByNewRow;
	private Row leftOrRightJoinAggGroupByDeleteRow;

	//JoinAgg
	private Row innerJoinAggGroupOldRow;
	private Row innerJoinAggGroupUpdatedOldRow;
	private Row innerJoinAggGroupNewRow;
	private Row innerJoinAggGroupDeleteRow;
	private Row leftOrRightJoinAggGroupOldRow;
	private Row leftOrRightJoinAggGroupUpdatedOldRow;
	private Row leftOrRightJoinAggGroupNewRow;
	private Row leftOrRightJoinAggGroupDeleteRow;

	public Stream() {
		resetJoinAggGroupByUpRows();
		resetJoinAggRows();
	}

	private void resetJoinAggRows() {

		innerJoinAggGroupOldRow = null;
		innerJoinAggGroupUpdatedOldRow = null;
		innerJoinAggGroupNewRow = null;
		innerJoinAggGroupDeleteRow = null;
		leftOrRightJoinAggGroupOldRow = null;
		leftOrRightJoinAggGroupUpdatedOldRow = null;
		leftOrRightJoinAggGroupNewRow = null;
		leftOrRightJoinAggGroupDeleteRow = null;

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

	public Row getInnerJoinAggGroupOldRow() {
		return innerJoinAggGroupOldRow;
	}

	public void setInnerJoinAggGroupOldRow(Row innerJoinAggGroupOldRow) {
		this.innerJoinAggGroupOldRow = innerJoinAggGroupOldRow;
	}

	public Row getInnerJoinAggGroupUpdatedOldRow() {
		return innerJoinAggGroupUpdatedOldRow;
	}

	public void setInnerJoinAggGroupUpdatedOldRow(
			Row innerJoinAggGroupUpdatedOldRow) {
		this.innerJoinAggGroupUpdatedOldRow = innerJoinAggGroupUpdatedOldRow;
	}

	public Row getInnerJoinAggGroupNewRow() {
		return innerJoinAggGroupNewRow;
	}

	public void setInnerJoinAggGroupNewRow(Row innerJoinAggGroupNewRow) {
		this.innerJoinAggGroupNewRow = innerJoinAggGroupNewRow;
	}

	public Row getInnerJoinAggGroupDeleteRow() {
		return innerJoinAggGroupDeleteRow;
	}

	public void setInnerJoinAggGroupDeleteRow(Row innerJoinAggGroupDeleteRow) {
		this.innerJoinAggGroupDeleteRow = innerJoinAggGroupDeleteRow;
	}

	public Row getLeftOrRightJoinAggGroupOldRow() {
		return leftOrRightJoinAggGroupOldRow;
	}

	public void setLeftOrRightJoinAggGroupOldRow(
			Row leftOrRightJoinAggGroupOldRow) {
		this.leftOrRightJoinAggGroupOldRow = leftOrRightJoinAggGroupOldRow;
	}

	public Row getLeftOrRightJoinAggGroupUpdatedOldRow() {
		return leftOrRightJoinAggGroupUpdatedOldRow;
	}

	public void setLeftOrRightJoinAggGroupUpdatedOldRow(
			Row leftOrRightJoinAggGroupUpdatedOldRow) {
		this.leftOrRightJoinAggGroupUpdatedOldRow = leftOrRightJoinAggGroupUpdatedOldRow;
	}

	public Row getLeftOrRightJoinAggGroupNewRow() {
		return leftOrRightJoinAggGroupNewRow;
	}

	public void setLeftOrRightJoinAggGroupNewRow(
			Row leftOrRightJoinAggGroupNewRow) {
		this.leftOrRightJoinAggGroupNewRow = leftOrRightJoinAggGroupNewRow;
	}

	public Row getLeftOrRightJoinAggGroupDeleteRow() {
		return leftOrRightJoinAggGroupDeleteRow;
	}

	public void setLeftOrRightJoinAggGroupDeleteRow(
			Row leftOrRightJoinAggGroupDeleteRow) {
		this.leftOrRightJoinAggGroupDeleteRow = leftOrRightJoinAggGroupDeleteRow;
	}


}
