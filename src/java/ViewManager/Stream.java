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
	private Row innerJoinAggOldRow;
	private Row innerJoinAggUpdatedOldRow;
	private Row innerJoinAggNewRow;
	private Row innerJoinAggDeleteRow;
	private Row leftOrRightJoinAggOldRow;
	private Row leftOrRightJoinAggUpdatedOldRow;
	private Row leftOrRightJoinAggNewRow;
	private Row leftOrRightJoinAggDeleteRow;

	//Delta
	private Row deltaUpdatedRow;
	private Row deltaDeletedRow;

	public Stream() {
		resetJoinAggGroupByUpRows();
		resetJoinAggRows();
		resetDeltaRows();
	}

	private void resetDeltaRows() {
		deltaUpdatedRow = null;
		deltaDeletedRow = null;
	}

	public void resetJoinAggRows() {

		innerJoinAggOldRow = null;
		innerJoinAggUpdatedOldRow = null;
		innerJoinAggNewRow = null;
		innerJoinAggDeleteRow = null;
		leftOrRightJoinAggOldRow = null;
		leftOrRightJoinAggUpdatedOldRow = null;
		leftOrRightJoinAggNewRow = null;
		leftOrRightJoinAggDeleteRow = null;

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

	public Row getInnerJoinAggOldRow() {
		return innerJoinAggOldRow;
	}

	public void setInnerJoinAggOldRow(Row innerJoinAggGroupOldRow) {
		this.innerJoinAggOldRow = innerJoinAggGroupOldRow;
	}

	public Row getInnerJoinAggUpdatedOldRow() {
		return innerJoinAggUpdatedOldRow;
	}

	public void setInnerJoinAggUpdatedOldRow(
			Row innerJoinAggGroupUpdatedOldRow) {
		this.innerJoinAggUpdatedOldRow = innerJoinAggGroupUpdatedOldRow;
	}

	public Row getInnerJoinAggNewRow() {
		return innerJoinAggNewRow;
	}

	public void setInnerJoinAggNewRow(Row innerJoinAggGroupNewRow) {
		this.innerJoinAggNewRow = innerJoinAggGroupNewRow;
	}

	public Row getInnerJoinAggDeleteRow() {
		return innerJoinAggDeleteRow;
	}

	public void setInnerJoinAggDeleteRow(Row innerJoinAggGroupDeleteRow) {
		this.innerJoinAggDeleteRow = innerJoinAggGroupDeleteRow;
	}

	public Row getLeftOrRightJoinAggOldRow() {
		return leftOrRightJoinAggOldRow;
	}

	public void setLeftOrRightJoinAggOldRow(
			Row leftOrRightJoinAggGroupOldRow) {
		this.leftOrRightJoinAggOldRow = leftOrRightJoinAggGroupOldRow;
	}

	public Row getLeftOrRightJoinAggUpdatedOldRow() {
		return leftOrRightJoinAggUpdatedOldRow;
	}

	public void setLeftOrRightJoinAggUpdatedOldRow(
			Row leftOrRightJoinAggGroupUpdatedOldRow) {
		this.leftOrRightJoinAggUpdatedOldRow = leftOrRightJoinAggGroupUpdatedOldRow;
	}

	public Row getLeftOrRightJoinAggNewRow() {
		return leftOrRightJoinAggNewRow;
	}

	public void setLeftOrRightJoinAggNewRow(
			Row leftOrRightJoinAggGroupNewRow) {
		this.leftOrRightJoinAggNewRow = leftOrRightJoinAggGroupNewRow;
	}

	public Row getLeftOrRightJoinAggDeleteRow() {
		return leftOrRightJoinAggDeleteRow;
	}

	public void setLeftOrRightJoinAggDeleteRow(
			Row leftOrRightJoinAggGroupDeleteRow) {
		this.leftOrRightJoinAggDeleteRow = leftOrRightJoinAggGroupDeleteRow;
	}

	public Row getDeltaUpdatedRow() {
		return deltaUpdatedRow;
	}

	public void setDeltaUpdatedRow(Row deltaUpdatedRow) {
		this.deltaUpdatedRow = deltaUpdatedRow;
	}

	public Row getDeltaDeletedRow() {
		return deltaDeletedRow;
	}

	public void setDeltaDeletedRow(Row deltaDeletedRow) {
		this.deltaDeletedRow = deltaDeletedRow;
	}


}
