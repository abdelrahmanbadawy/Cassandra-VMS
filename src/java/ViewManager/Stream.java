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

	//Preaggregation
	private Row updatedPreaggRow;
	private Row updatedPreaggRowDeleted;
	private Row updatedPreaggRowChangeAK;
	//Preaggregation delete
	private Row deletePreaggRow;


	//Reverse Join
	private Row reverseJoinUpdateNewRow;
	private Row reverseJoinUpadteOldRow;
	private Row reverseJoinUpdatedOldRow_changeJoinKey;
	private Row reverseJoinDeleteNewRow;
	private Row revereJoinDeleteOldRow;



	public Stream() {
		resetJoinAggGroupByUpRows();
		resetJoinAggRows();
		resetDeltaRows();
		resetPreaggregationRows();
		resetReverseJoinRows();
	}

	public void resetReverseJoinRows() {
		reverseJoinUpdateNewRow = null;
		reverseJoinUpadteOldRow = null;
		reverseJoinUpdatedOldRow_changeJoinKey= null;
		reverseJoinDeleteNewRow= null;
		revereJoinDeleteOldRow = null;
	}

	public void resetPreaggregationRows() {
		updatedPreaggRow = null;
		updatedPreaggRowDeleted = null;
		updatedPreaggRowChangeAK = null;
		deletePreaggRow = null;

	}

	public void resetDeltaRows() {
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

	public Row getUpdatedPreaggRow() {
		return updatedPreaggRow;
	}

	public void setUpdatedPreaggRow(Row updatedPreaggRow) {
		this.updatedPreaggRow = updatedPreaggRow;
	}

	public Row getUpdatedPreaggRowDeleted() {
		return updatedPreaggRowDeleted;
	}

	public void setUpdatedPreaggRowDeleted(Row updatedPreaggRowDeleted) {
		this.updatedPreaggRowDeleted = updatedPreaggRowDeleted;
	}

	public Row getUpdatedPreaggRowChangeAK() {
		return updatedPreaggRowChangeAK;
	}

	public void setUpdatedPreaggRowChangeAK(Row updatedPreaggRowChangeAK) {
		this.updatedPreaggRowChangeAK = updatedPreaggRowChangeAK;
	}

	public Row getReverseJoinUpdateNewRow() {
		return reverseJoinUpdateNewRow;
	}

	public void setReverseJoinUpdateNewRow(Row reverseJoinUpdateNewRow) {
		this.reverseJoinUpdateNewRow = reverseJoinUpdateNewRow;
	}

	public Row getReverseJoinUpadteOldRow() {
		return reverseJoinUpadteOldRow;
	}

	public void setReverseJoinUpadteOldRow(Row reverseJoinUpadteOldRow) {
		this.reverseJoinUpadteOldRow = reverseJoinUpadteOldRow;
	}

	public Row getReverseJoinUpdatedOldRow_changeJoinKey() {
		return reverseJoinUpdatedOldRow_changeJoinKey;
	}

	public void setReverseJoinUpdatedOldRow_changeJoinKey(
			Row reverseJoinUpdatedOldRow_changeJoinKey) {
		this.reverseJoinUpdatedOldRow_changeJoinKey = reverseJoinUpdatedOldRow_changeJoinKey;
	}

	public Row getReverseJoinDeleteNewRow() {
		return reverseJoinDeleteNewRow;
	}

	public void setReverseJoinDeleteNewRow(Row reverseJoinDeleteNewRow) {
		this.reverseJoinDeleteNewRow = reverseJoinDeleteNewRow;
	}

	public Row getRevereJoinDeleteOldRow() {
		return revereJoinDeleteOldRow;
	}

	public void setRevereJoinDeleteOldRow(Row revereJoinDeleteOldRow) {
		this.revereJoinDeleteOldRow = revereJoinDeleteOldRow;
	}


	public Row getDeletePreaggRow() {
		return deletePreaggRow;
	}

	public void setDeletePreaggRow(Row deletePreaggRow) {
		this.deletePreaggRow = deletePreaggRow;
	}


}
