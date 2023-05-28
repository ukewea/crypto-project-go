package cryptocompare

import (
	"testing"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
)

func TestRemoveNotReadyData(t *testing.T) {
	tests := []struct {
		name string
		data []OHLCVData
		want []OHLCVData
	}{
		{
			name: "all data row ready",
			data: []OHLCVData{
				{VolumeFrom: decimal.NewFromInt(1)},
				{VolumeFrom: decimal.NewFromInt(2)},
			},
			want: []OHLCVData{
				{VolumeFrom: decimal.NewFromInt(1)},
				{VolumeFrom: decimal.NewFromInt(2)},
			},
		},
		{
			name: "last data row not ready",
			data: []OHLCVData{
				{VolumeFrom: decimal.NewFromInt(1)},
				{VolumeFrom: decimal.NewFromInt(2)},
				{VolumeFrom: decimal.NewFromInt(0)},
			},
			want: []OHLCVData{
				{VolumeFrom: decimal.NewFromInt(1)},
				{VolumeFrom: decimal.NewFromInt(2)},
			},
		},
		{
			name: "no data",
			data: []OHLCVData{},
			want: []OHLCVData{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := removeNotReadyData(tt.data)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestIsVolumeFromZeroInDataSet(t *testing.T) {
	tests := []struct {
		name string
		data []OHLCVData
		want bool
	}{
		{
			name: "all volumesfrom are zero",
			data: []OHLCVData{
				{VolumeFrom: decimal.Zero},
				{VolumeFrom: decimal.Zero},
			},
			want: true,
		},
		{
			name: "some volumesfrom are non-zero",
			data: []OHLCVData{
				{VolumeFrom: decimal.Zero},
				{VolumeFrom: decimal.NewFromInt(1)},
			},
			want: false,
		},
		{
			name: "all volumesfrom are non-zero",
			data: []OHLCVData{
				{VolumeFrom: decimal.NewFromInt(1)},
				{VolumeFrom: decimal.NewFromInt(2)},
			},
			want: false,
		},
		{
			name: "no data",
			data: []OHLCVData{},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isVolumeFromZeroInDataSet(tt.data)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestSortByTime(t *testing.T) {
	data := []OHLCVData{
		{
			Time: 3,
		},
		{
			Time: 1,
		},
		{
			Time: 2,
		},
	}

	sortByTime(data)

	if data[0].Time != 1 || data[1].Time != 2 || data[2].Time != 3 {
		t.Errorf("sortByTime failed, got: %v, want: %v.", []int64{data[0].Time, data[1].Time, data[2].Time}, []int64{1, 2, 3})
	}

	data = []OHLCVData{
		{
			Time:       3,
			VolumeFrom: decimal.NewFromInt(0),
		},
		{
			Time:       1,
			VolumeFrom: decimal.NewFromInt(4),
		},
		{
			Time:       1,
			VolumeFrom: decimal.NewFromInt(3),
		},
		{
			Time:       2,
			VolumeFrom: decimal.NewFromInt(2),
		},
		{
			Time:       2,
			VolumeFrom: decimal.NewFromInt(1),
		},
	}

	sortByTime(data)

	if !data[0].VolumeFrom.Equal(decimal.NewFromInt((4))) ||
		!data[1].VolumeFrom.Equal(decimal.NewFromInt((3))) ||
		!data[2].VolumeFrom.Equal(decimal.NewFromInt((2))) ||
		!data[3].VolumeFrom.Equal(decimal.NewFromInt((1))) ||
		!data[4].VolumeFrom.Equal(decimal.NewFromInt((0))) {
		t.Errorf("sortByTime stable check failed, got: %v, want: %v.",
			[]decimal.Decimal{
				data[0].VolumeFrom,
				data[1].VolumeFrom,
				data[2].VolumeFrom,
				data[3].VolumeFrom,
				data[4].VolumeFrom,
			},
			[]decimal.Decimal{
				decimal.NewFromInt((4)),
				decimal.NewFromInt((3)),
				decimal.NewFromInt((2)),
				decimal.NewFromInt((1)),
				decimal.NewFromInt((0)),
			})
	}
}
