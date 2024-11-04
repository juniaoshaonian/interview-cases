package case31

import (
	"github.com/stretchr/testify/require"
	"interview-cases/test"
	"testing"
)

func TestCase31(t *testing.T) {
	db := test.InitDB()
	err := InitTable(db)
	require.NoError(t, err)
	err = InitData(db)
	require.NoError(t, err)
}
