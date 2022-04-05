#include <postgres.h>
#include <fmgr.h>
#include <utils/array.h>
#include <catalog/pg_type.h>
#include <utils/lsyscache.h>
#include <math.h>
#include <string.h>
#include <common/int.h>
#include <utils/builtins.h>
#include <utils/typcache.h>
#include <funcapi.h>


PG_MODULE_MAGIC;

Datum min_to_max_sfunc(PG_FUNCTION_ARGS);
Datum min_to_max_ffunc(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(min_to_max_sfunc);
PG_FUNCTION_INFO_V1(min_to_max_ffunc);

char  *fldsep;

typedef union pgnum {
	  int16 i16;
	  int32 i32;
	  int64 i64;
	  float4 f4;
	  float8 f8;
	} pgnum; 
	
static text *
	 array_to_text_internal(FunctionCallInfo fcinfo, ArrayType *v,
							const char *fldsep)
	 {
		 text       *result;
		 int         nitems,
					*dims,
					 ndims;
		 Oid         element_type;
		 int         typlen;
		 bool        typbyval;
		 char        typalign;
		 StringInfoData buf;
		 bool        printed = false;
		 char       *p;
		 int         i;
		 ArrayMetaState *my_extra;
	  
		 ndims = ARR_NDIM(v);
		 dims = ARR_DIMS(v);
		 nitems = ArrayGetNItems(ndims, dims); // we know this will be 2 only 
	  
		 if (nitems == 0) 
			 return cstring_to_text_with_len("", 0);
	  
		 element_type = ARR_ELEMTYPE(v);  
		 /*Initialize a StringInfoData struct (with previously undefined contents) to describe an empty string. initsize will be 1024*/
		 initStringInfo(&buf);
	  
		/* actual structure cache type metadata needed for array manipulation*/
		 my_extra = (ArrayMetaState *) fcinfo->flinfo->fn_extra;
		 
		 if (my_extra == NULL)
		 {
			
			 /* MemoryContextAlloc - >  Allocate space within the specified context.*/
			 fcinfo->flinfo->fn_extra = MemoryContextAlloc(fcinfo->flinfo->fn_mcxt,sizeof(ArrayMetaState));
			/* actual structure cache type metadata needed for array manipulation*/												 
			 my_extra = (ArrayMetaState *) fcinfo->flinfo->fn_extra;
			/* setting Ones compliment */
			 my_extra->element_type = ~element_type;
		 }
	  
		 if (my_extra->element_type != element_type)
		 {
			 
			 /* given the type OID, return typlen, typbyval, typalign,
					typdelim, typioparam, and IO function OID. The IO function
                    returned is controlled by IOFuncSelector */
			 get_type_io_data(element_type, IOFunc_output,
							  &my_extra->typlen, &my_extra->typbyval,
							  &my_extra->typalign, &my_extra->typdelim,
							  &my_extra->typioparam, &my_extra->typiofunc);
			/* Fill a FmgrInfo struct(&my_extra->proc), specifying a memory context in which its
					 subsidiary data should go. */
			 fmgr_info_cxt(my_extra->typiofunc, &my_extra->proc,
						   fcinfo->flinfo->fn_mcxt);
			 my_extra->element_type = element_type;
		 }
		 
		 typlen = my_extra->typlen;
		 typbyval = my_extra->typbyval;
		 typalign = my_extra->typalign;
		 
		 
		 /* ARR_DATA_PTR -> Returns a pointer to the actual array data.*/
		 p = ARR_DATA_PTR(v);
	  
		 for (i = 0; i < nitems; i++)
		 {
			 Datum       itemvalue;
			 char       *value;
			 
			 /* fetch_att-> get attribute value of array pointer by type and len*/
			 itemvalue = fetch_att(p, typbyval, typlen);
  
			 /* OutputFunctionCall -> Returns C string value*/
			 value = OutputFunctionCall(&my_extra->proc, itemvalue);
  
			 if (printed)
				 /* * Format text data under the control of fmt (an sprintf-style format string)
				  * and append it to whatever is already in str.  More space is allocated
				  * to str if necessary.  This is sort of like a combination of sprintf and
				  * strcat.*/
				 appendStringInfo(&buf, "%s%s", fldsep, value);
			 else
				  /* Append a null-terminated string to str.*/
				 appendStringInfoString(&buf, value);
			 printed = true;
  
			  /* att_addlength_pointer -> increments the given offset by the space needed */
			  p = att_addlength_pointer(p, typlen, p);    // increase the offset of P to get next value based on pointer
			  /*att_align_nominal -> aligns the given offset as needed for a datum of alignment requirement*/
			  p = (char *) att_align_nominal(p, typalign); // align the p with typealign so that next fetch_att get exact itemvalue
		 }
	  
		 result = cstring_to_text_with_len(buf.data, buf.len);
		 pfree(buf.data);
	  
		 return result;
	 }
	

Datum
min_to_max_sfunc(PG_FUNCTION_ARGS)
	{
		/* Get the actual type OID of a specific function argument starts from zero */
		Oid   arg_type = get_fn_expr_argtype(fcinfo->flinfo, 1);
		MemoryContext aggcontext;
		ArrayBuildState *state;
		Datum   data;
		
		if (!PG_ARGISNULL(2))
         fldsep = text_to_cstring(PG_GETARG_TEXT_PP(2));
		else
         fldsep = "->";

		if (arg_type == InvalidOid)
		 ereport(ERROR,
			 (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			  errmsg("Invalid Parameter Value")));

		// AggCheckCallContext -> checks that func called in agg context or not
		if (!AggCheckCallContext(fcinfo, &aggcontext))
			elog(ERROR, "min_to_max_sfunc called in non-aggregate context");
		
		
		if (PG_ARGISNULL(0))
		 /* initArrayResult - initialize an empty ArrayBuildState
    		element_type is the array element type (must be a valid array element type)
    		rcontext is where to keep working state
    		subcontext is a flag determining whether to use a separate memory context */
		 state = initArrayResult(arg_type, aggcontext, false);
		else
		 /* Build Array from input */
		 state = (ArrayBuildState *) PG_GETARG_POINTER(0);

		data = PG_ARGISNULL(1) ? (Datum) 0 : PG_GETARG_DATUM(1);
		/*
		  accumArrayResult - accumulate one (more) Datum for an array result
		  astate is working state (can be NULL on first call)
		  dvalue/disnull represent the new Datum to append to the array
		  element_type is the Datum's type (must be a valid array element type)
		  rcontext is where to keep working state
		*/
		state = accumArrayResult(state,
						  data,
						  PG_ARGISNULL(1),
						  arg_type,
						  aggcontext);
						  
		PG_RETURN_POINTER(state);
	}


Datum
min_to_max_ffunc(PG_FUNCTION_ARGS)
	{

		ArrayBuildState *state;
		int  dims[1],lbs[1],valsLength,i;
		ArrayType *vals;
		Oid valsType;
		int16 valsTypeWidth,retTypeWidth;
		bool valsTypeByValue,retTypeByValue,resultIsNull = true,*valsNullFlags;
		char valsTypeAlignmentCode,retTypeAlignmentCode;
		Datum *valsContent,retContent[2];
		pgnum minV, maxV;
		bool retNulls[2] = {true, true};
		ArrayType* retArray;
		
		
		

		Assert(AggCheckCallContext(fcinfo, NULL));

		state = PG_ARGISNULL(0) ? NULL : (ArrayBuildState *) PG_GETARG_POINTER(0);

		if (state == NULL) 
				PG_RETURN_NULL();       
			
		dims[0] = state->nelems;
		lbs[0] = 1;
		
		vals = (ArrayType *)makeMdArrayResult(state, 1, dims, lbs,
						CurrentMemoryContext,
						false);

		if (ARR_NDIM(vals) > 1) 
			ereport(ERROR,(errmsg("Not received one dimensional array ")));
		

		valsType = ARR_ELEMTYPE(vals);

		if (valsType != INT2OID &&
			valsType != INT4OID &&
			valsType != INT8OID &&
			valsType != FLOAT4OID &&
			valsType != FLOAT8OID) {
				ereport(ERROR, (errmsg("Supported Datatypes are SMALLINT, INTEGER, BIGINT, REAL, or DOUBLE PRECISION.")));
			}

		/*ARR_DIMS returns a pointer to an array of array dimensions (number of elements along the various array axes).*/
		valsLength = (ARR_DIMS(vals))[0];

		/* for given type OID, it return typlen, typbyval, typalign. */
		get_typlenbyvalalign(valsType, &valsTypeWidth, &valsTypeByValue, &valsTypeAlignmentCode);

		/* deconstruct_array -> simple method for extracting data from an array stores into valsContent*/
		deconstruct_array(vals, valsType, valsTypeWidth, valsTypeByValue, valsTypeAlignmentCode,
			&valsContent, &valsNullFlags, &valsLength);
			
		/* each get_typlenbyvalalign with OID return typewidth,typebyval,typealign */	
		switch (valsType) {
		case INT2OID:
			for (i = 0; i < valsLength; i++) {
				if (valsNullFlags[i]) {
				  continue;
				} else if (resultIsNull) {
				  minV.i16 = DatumGetInt16(valsContent[i]);
				  maxV.i16 = DatumGetInt16(valsContent[i]);
				  resultIsNull = false;
				} else {
				  if (DatumGetInt16(valsContent[i]) < minV.i16) minV.i16 = DatumGetInt16(valsContent[i]);
				  if (DatumGetInt16(valsContent[i]) > maxV.i16) maxV.i16 = DatumGetInt16(valsContent[i]);
				}
			}
			retContent[0] = Int16GetDatum(minV.i16);
			retContent[1] = Int16GetDatum(maxV.i16);
			get_typlenbyvalalign(INT2OID, &retTypeWidth, &retTypeByValue, &retTypeAlignmentCode);
			break;
		case INT4OID:
			for (i = 0; i < valsLength; i++) {
				if (valsNullFlags[i]) {
				  continue;
				} else if (resultIsNull) {
				  minV.i32 = DatumGetInt32(valsContent[i]);
				  maxV.i32 = DatumGetInt32(valsContent[i]);
				  resultIsNull = false;
				} else {
				  if (DatumGetInt32(valsContent[i]) < minV.i32) minV.i32 = DatumGetInt32(valsContent[i]);
				  if (DatumGetInt32(valsContent[i]) > maxV.i32) maxV.i32 = DatumGetInt32(valsContent[i]);
				}
			}
			retContent[0] = Int32GetDatum(minV.i32);
			retContent[1] = Int32GetDatum(maxV.i32);
			get_typlenbyvalalign(INT4OID, &retTypeWidth, &retTypeByValue, &retTypeAlignmentCode);
			break;
		case INT8OID:
			for (i = 0; i < valsLength; i++) {
				if (valsNullFlags[i]) {
				  continue;
				} else if (resultIsNull) {
				  minV.i64 = DatumGetInt64(valsContent[i]);
				  maxV.i64 = DatumGetInt64(valsContent[i]);
				  resultIsNull = false;
				} else {
				  if (DatumGetInt64(valsContent[i]) < minV.i64) minV.i64 = DatumGetInt64(valsContent[i]);
				  if (DatumGetInt64(valsContent[i]) > maxV.i64) maxV.i64 = DatumGetInt64(valsContent[i]);
				}
			}
			retContent[0] = Int64GetDatum(minV.i64);
			retContent[1] = Int64GetDatum(maxV.i64);
			get_typlenbyvalalign(INT8OID, &retTypeWidth, &retTypeByValue, &retTypeAlignmentCode);
			break;
		case FLOAT4OID:
			for (i = 0; i < valsLength; i++) {
				if (valsNullFlags[i]) {
				  continue;
				} else if (resultIsNull) {
				  minV.f4 = DatumGetFloat4(valsContent[i]);
				  maxV.f4 = DatumGetFloat4(valsContent[i]);
				  resultIsNull = false;
				} else {
				  if (DatumGetFloat4(valsContent[i]) < minV.f4) minV.f4 = DatumGetFloat4(valsContent[i]);
				  if (DatumGetFloat4(valsContent[i]) > maxV.f4) maxV.f4 = DatumGetFloat4(valsContent[i]);
				}
			}
			retContent[0] = Float4GetDatum(minV.f4);
			retContent[1] = Float4GetDatum(maxV.f4);
			get_typlenbyvalalign(FLOAT4OID, &retTypeWidth, &retTypeByValue, &retTypeAlignmentCode);
			break;
		case FLOAT8OID:
			for (i = 0; i < valsLength; i++) {
				if (valsNullFlags[i]) {
				  continue;
				} else if (resultIsNull) {
				  minV.f8 = DatumGetFloat8(valsContent[i]);
				  maxV.f8 = DatumGetFloat8(valsContent[i]);
				  resultIsNull = false;
				} else {
				  if (DatumGetFloat8(valsContent[i]) < minV.f8) minV.f8 = DatumGetFloat8(valsContent[i]);
				  if (DatumGetFloat8(valsContent[i]) > maxV.f8) maxV.f8 = DatumGetFloat8(valsContent[i]);
				}
			}
			retContent[0] = Float8GetDatum(minV.f8);
			retContent[1] = Float8GetDatum(maxV.f8);
			get_typlenbyvalalign(FLOAT8OID, &retTypeWidth, &retTypeByValue, &retTypeAlignmentCode);
			break;
		default:
			ereport(ERROR, (errmsg("Supported Datatypes are SMALLINT, INTEGER, BIGINT, REAL, or DOUBLE PRECISION.")));
		}
		

		  lbs[0] = 1;
		  dims[0] = 2;
		  if (!resultIsNull) {
			retNulls[0] = false;
			retNulls[1] = false;
		  }
		  
		  
		/*
		  * construct_md_array   --- simple method for constructing an array object
		  *                          with arbitrary dimensions and possible NULLs
		  *
		  * elems: array of Datum items to become the array contents
		  * nulls: array of is-null flags (can be NULL if no nulls)
		  * ndims: number of dimensions
		  * dims: integer array with size of each dimension
		  * lbs: integer array with lower bound of each dimension
		  * elmtype, elmlen, elmbyval, elmalign: info for the datatype of the items*/
		  
		retArray = construct_md_array(retContent, retNulls, 1, dims, lbs, valsType, retTypeWidth, retTypeByValue, retTypeAlignmentCode);

		
		PG_RETURN_TEXT_P(array_to_text_internal(fcinfo, retArray, fldsep));
			
	}
