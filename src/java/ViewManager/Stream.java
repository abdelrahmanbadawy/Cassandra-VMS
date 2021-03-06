package ViewManager;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;

import org.json.simple.JSONObject;

import com.datastax.driver.core.Row;

public class Stream implements Serializable {

	//JoinAggGroupBy
	private CustomizedRow updatedJoinAggGroupByRowOldState;
	private CustomizedRow updatedJoinAggGroupByRow;
	private CustomizedRow updatedJoinAggGroupByRowDeleted;

	//JoinAgg
	private CustomizedRow innerJoinAggOldRow;
	private CustomizedRow innerJoinAggUpdatedOldRow;
	private CustomizedRow innerJoinAggNewRow;
	private CustomizedRow innerJoinAggDeleteRow;
	private CustomizedRow leftOrRightJoinAggOldRow;
	private CustomizedRow leftOrRightJoinAggUpdatedOldRow;
	private CustomizedRow leftOrRightJoinAggNewRow;
	private CustomizedRow leftOrRightJoinAggDeleteRow;

	//Delta
	private CustomizedRow deltaUpdatedRow;
	private CustomizedRow deltaDeletedRow;

	//Preaggregation
	private CustomizedRow updatedPreaggRow;
	private CustomizedRow updatedPreaggRowOldState;
	private CustomizedRow updatedPreaggRowDeleted;

	//Reverse Join
	private CustomizedRow reverseJoinUpdateNewRow;
	private CustomizedRow reverseJoinUpadteOldRow;
	private CustomizedRow reverseJoinDeleteNewRow;
	private CustomizedRow revereJoinDeleteOldRow;

	private String baseTable;

	private boolean isDeleteOperation;

	private JSONObject deltaJSON;
	
	private boolean changeInJoinKey;
	
	//case change in join key --> determines if the opposite listitem of the old join key value is empty
	private boolean oppositeSizeZero;


	public Stream() {
		isDeleteOperation = false;
		resetJoinAggGroupByUpRows();
		resetJoinAggRows();
		resetDeltaRows();
		resetPreaggregationRows();
		resetReverseJoinRows();
	}


	public void resetReverseJoinRows() {
		reverseJoinUpdateNewRow = null;
		reverseJoinUpadteOldRow = null;
		//reverseJoinUpdatedOldRow_changeJoinKey= null;
		reverseJoinDeleteNewRow= null;
		revereJoinDeleteOldRow = null;
	}

	public void resetPreaggregationRows() {
		updatedPreaggRow = null;
		updatedPreaggRowDeleted = null;
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


	public void resetJoinAggGroupByUpRows(){
		setUpdatedJoinAggGroupByRow(null);
		setUpdatedJoinAggGroupByRowDeleted(null);
	}


	public CustomizedRow getInnerJoinAggOldRow() {
		return innerJoinAggOldRow;
	}

	public void setInnerJoinAggOldRow(CustomizedRow innerJoinAggGroupOldRow) {
		this.innerJoinAggOldRow = innerJoinAggGroupOldRow;
	}

	public CustomizedRow getInnerJoinAggUpdatedOldRow() {
		return innerJoinAggUpdatedOldRow;
	}

	public void setInnerJoinAggUpdatedOldRow(
			CustomizedRow innerJoinAggGroupUpdatedOldRow) {
		this.innerJoinAggUpdatedOldRow = innerJoinAggGroupUpdatedOldRow;
	}

	public CustomizedRow getInnerJoinAggNewRow() {
		return innerJoinAggNewRow;
	}

	public void setInnerJoinAggNewRow(CustomizedRow innerJoinAggGroupNewRow) {
		this.innerJoinAggNewRow = innerJoinAggGroupNewRow;
	}

	public CustomizedRow getInnerJoinAggDeleteRow() {
		return innerJoinAggDeleteRow;
	}

	public void setInnerJoinAggDeleteRow(CustomizedRow innerJoinAggGroupDeleteRow) {
		this.innerJoinAggDeleteRow = innerJoinAggGroupDeleteRow;
	}

	public CustomizedRow getLeftOrRightJoinAggOldRow() {
		return leftOrRightJoinAggOldRow;
	}

	public void setLeftOrRightJoinAggOldRow(
			CustomizedRow leftOrRightJoinAggGroupOldRow) {
		this.leftOrRightJoinAggOldRow = leftOrRightJoinAggGroupOldRow;
	}

	public CustomizedRow getLeftOrRightJoinAggUpdatedOldRow() {
		return leftOrRightJoinAggUpdatedOldRow;
	}

	public void setLeftOrRightJoinAggUpdatedOldRow(
			CustomizedRow leftOrRightJoinAggGroupUpdatedOldRow) {
		this.leftOrRightJoinAggUpdatedOldRow = leftOrRightJoinAggGroupUpdatedOldRow;
	}

	public CustomizedRow getLeftOrRightJoinAggNewRow() {
		return leftOrRightJoinAggNewRow;
	}

	public void setLeftOrRightJoinAggNewRow(
			CustomizedRow leftOrRightJoinAggGroupNewRow) {
		this.leftOrRightJoinAggNewRow = leftOrRightJoinAggGroupNewRow;
	}

	public CustomizedRow getLeftOrRightJoinAggDeleteRow() {
		return leftOrRightJoinAggDeleteRow;
	}

	public void setLeftOrRightJoinAggDeleteRow(
			CustomizedRow leftOrRightJoinAggGroupDeleteRow) {
		this.leftOrRightJoinAggDeleteRow = leftOrRightJoinAggGroupDeleteRow;
	}

	public CustomizedRow getDeltaUpdatedRow() {
		return deltaUpdatedRow;
	}

	public void setDeltaUpdatedRow(CustomizedRow deltaUpdatedRow) {
		this.deltaUpdatedRow = deltaUpdatedRow;
	}

	public CustomizedRow getDeltaDeletedRow() {
		return deltaDeletedRow;
	}

	public void setDeltaDeletedRow(CustomizedRow deltaDeletedRow) {
		this.deltaDeletedRow = deltaDeletedRow;
	}

	public CustomizedRow getUpdatedPreaggRow() {
		return updatedPreaggRow;
	}

	public void setUpdatedPreaggRow(CustomizedRow updatedPreaggRow) {
		this.updatedPreaggRow = updatedPreaggRow;
	}

	public CustomizedRow getUpdatedPreaggRowDeleted() {
		return updatedPreaggRowDeleted;
	}

	public void setUpdatedPreaggRowDeleted(CustomizedRow updatedPreaggRowDeleted) {
		this.updatedPreaggRowDeleted = updatedPreaggRowDeleted;
	}

	public CustomizedRow getReverseJoinUpdateNewRow() {
		return reverseJoinUpdateNewRow;
	}

	public void setReverseJoinUpdateNewRow(CustomizedRow reverseJoinUpdateNewRow) {
		this.reverseJoinUpdateNewRow = reverseJoinUpdateNewRow;
	}

	public CustomizedRow getReverseJoinUpadteOldRow() {
		return reverseJoinUpadteOldRow;
	}

	public void setReverseJoinUpadteOldRow(CustomizedRow reverseJoinUpadteOldRow) {
		this.reverseJoinUpadteOldRow = reverseJoinUpadteOldRow;
	}



	public CustomizedRow getReverseJoinDeleteNewRow() {
		return reverseJoinDeleteNewRow;
	}

	public void setReverseJoinDeleteNewRow(CustomizedRow reverseJoinDeleteNewRow) {
		this.reverseJoinDeleteNewRow = reverseJoinDeleteNewRow;
	}

	public CustomizedRow getRevereJoinDeleteOldRow() {
		return revereJoinDeleteOldRow;
	}

	public void setRevereJoinDeleteOldRow(CustomizedRow revereJoinDeleteOldRow) {
		this.revereJoinDeleteOldRow = revereJoinDeleteOldRow;
	}

	public String getBaseTable() {
		return baseTable;
	}

	public void setBaseTable(String baseTable) {
		this.baseTable = baseTable;
	}

	public boolean isDeleteOperation() {
		return isDeleteOperation;
	}

	public void setDeleteOperation(boolean isDeleteOperation) {
		this.isDeleteOperation = isDeleteOperation;
	}


	public JSONObject getDeltaJSON() {
		return deltaJSON;
	}


	public void setDeltaJSON(JSONObject deltaJSON) {
		this.deltaJSON = deltaJSON;
	}


	public CustomizedRow getUpdatedJoinAggGroupByRow() {
		return updatedJoinAggGroupByRow;
	}


	public void setUpdatedJoinAggGroupByRow(CustomizedRow updatedJoinAggGroupByRow) {
		this.updatedJoinAggGroupByRow = updatedJoinAggGroupByRow;
	}


	public CustomizedRow getUpdatedJoinAggGroupByRowDeleted() {
		return updatedJoinAggGroupByRowDeleted;
	}


	public void setUpdatedJoinAggGroupByRowDeleted(
			CustomizedRow updatedJoinAggGroupByRowDeleted) {
		this.updatedJoinAggGroupByRowDeleted = updatedJoinAggGroupByRowDeleted;
	}


	public CustomizedRow getUpdatedPreaggRowOldState() {
		return updatedPreaggRowOldState;
	}


	public void setUpdatedPreaggRowOldState(CustomizedRow updatedPreaggRowOldState) {
		this.updatedPreaggRowOldState = updatedPreaggRowOldState;
	}


	public CustomizedRow getUpdatedJoinAggGroupByRowOldState() {
		return updatedJoinAggGroupByRowOldState;
	}


	public void setUpdatedJoinAggGroupByRowOldState(
			CustomizedRow updatedJoinAggGroupByRowOldState) {
		this.updatedJoinAggGroupByRowOldState = updatedJoinAggGroupByRowOldState;
	}


	public boolean isChangeInJoinKey() {
		return changeInJoinKey;
	}


	public void setChangeInJoinKey(boolean changeInJoinKey) {
		this.changeInJoinKey = changeInJoinKey;
	}


	public boolean isOppositeSizeZero() {
		return oppositeSizeZero;
	}


	public void setOppositeSizeZero(boolean oppositeSizeZero) {
		this.oppositeSizeZero = oppositeSizeZero;
	}

}
