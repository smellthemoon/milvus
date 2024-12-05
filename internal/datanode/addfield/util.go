// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package addfield

import (
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

func GenerateInsertData(fieldSchemas []*schemapb.FieldSchema, numRows int64) (*storage.InsertData, error) {
	insertData, err := storage.NewInsertData(&schemapb.CollectionSchema{Fields: fieldSchemas})
	if err != nil {
		return nil, err
	}
	for _, fieldSchema := range fieldSchemas {
		fieldData, err := storage.NewFieldData(fieldSchema.GetDataType(), fieldSchema, int(numRows))
		if err != nil {
			return nil, err
		}
		validData := make([]bool, numRows)
		switch fieldSchema.GetDataType() {
		case schemapb.DataType_Bool:
			data := make([]bool, numRows)
			if fieldSchema.GetDefaultValue() != nil {
				fillWithDefaultValue(data, validData, fieldSchema.DefaultValue.GetBoolData())
			}
			fieldData.AppendRows(data, validData)
			break
		case schemapb.DataType_Int8:
			data := make([]int8, numRows)
			if fieldSchema.GetDefaultValue() != nil {
				fillWithDefaultValue(data, validData, int8(fieldSchema.DefaultValue.GetIntData()))
			}
			fieldData.AppendRows(data, validData)
			break
		case schemapb.DataType_Int16:
			data := make([]int16, numRows)
			if fieldSchema.GetDefaultValue() != nil {
				fillWithDefaultValue(data, validData, int16(fieldSchema.DefaultValue.GetIntData()))
			}
			fieldData.AppendRows(data, validData)
			break
		case schemapb.DataType_Int32:
			data := make([]int32, numRows)
			if fieldSchema.GetDefaultValue() != nil {
				fillWithDefaultValue(data, validData, fieldSchema.DefaultValue.GetIntData())
			}
			fieldData.AppendRows(data, validData)
			break
		case schemapb.DataType_Int64:
			data := make([]int64, numRows)
			if fieldSchema.GetDefaultValue() != nil {
				fillWithDefaultValue(data, validData, fieldSchema.DefaultValue.GetLongData())
			}
			fieldData.AppendRows(data, validData)
			break
		case schemapb.DataType_Float:
			data := make([]float32, numRows)
			if fieldSchema.GetDefaultValue() != nil {
				fillWithDefaultValue(data, validData, fieldSchema.DefaultValue.GetFloatData())
			}
			fieldData.AppendRows(data, validData)
			break
		case schemapb.DataType_Double:
			data := make([]float64, numRows)
			if fieldSchema.GetDefaultValue() != nil {
				fillWithDefaultValue(data, validData, fieldSchema.DefaultValue.GetDoubleData())
			}
			fieldData.AppendRows(data, validData)
			break
		case schemapb.DataType_VarChar, schemapb.DataType_String:
			data := make([]string, numRows)
			if fieldSchema.GetDefaultValue() != nil {
				fillWithDefaultValue(data, validData, fieldSchema.DefaultValue.GetStringData())
			}
			fieldData.AppendRows(data, validData)
			break
		default:
			return nil, merr.WrapErrImportFailed(fmt.Sprintf("unsupported data type '%s' for field '%s'",
				fieldSchema.GetDataType().String(), fieldSchema.GetName()))
		}
		insertData.Data[fieldSchema.GetFieldID()] = fieldData
	}
	return insertData, nil
}

func fillWithDefaultValue[T any](data []T, validData []bool, value T) {
	for i := 0; i < len(data); i++ {
		data[i] = value
		validData[i] = true
	}
	return
}
