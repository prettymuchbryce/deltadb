package db

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreateCollection(t *testing.T) {
	context := NewTestContext(t)

	hash, err := CreateCollection(context, "tests", "this is a test", "testCollection")

	assert.NoError(t, err)

	assert.Equal(t, 1, len(context.appendix.Commits))

	assert.Equal(t, context.appendix.Commits[0].Operation.Type, OpCreateCollection)

	assert.Equal(t, context.appendix.Commits[0].Hash, hash)

	err, collections := ListCollections(context, context.GetLatestHash())

	assert.NoError(t, err)

	assert.Equal(t, 1, len(collections))

	assert.Equal(t, "testCollection", collections[0])
}

func TestDeleteCollection(t *testing.T) {
	context := NewTestContext(t)

	hash, err := CreateCollection(context, "tests", "this is a test", "testCollection")

	assert.NoError(t, err)

	assert.Equal(t, 1, len(context.appendix.Commits))

	assert.Equal(t, context.appendix.Commits[0].Operation.Type, OpCreateCollection)

	assert.Equal(t, context.appendix.Commits[0].Hash, hash)

	err, collections := ListCollections(context, context.GetLatestHash())

	assert.NoError(t, err)

	assert.Equal(t, 1, len(collections))

	hash, err = SetDocument(context, "tests", "this is a test", "testCollection", "testDocument", "My great test document")

	assert.Equal(t, context.appendix.Commits[1].Hash, hash)

	assert.NoError(t, err)

	hash, err = DeleteCollection(context, "tests", "this is a test", "testCollection")

	assert.Equal(t, context.appendix.Commits[2].Hash, hash)

	assert.NoError(t, err)

	err, collections = ListCollections(context, context.GetLatestHash())

	assert.NoError(t, err)

	assert.Equal(t, 0, len(collections))

	assert.Equal(t, 3, len(context.appendix.Commits))

	assert.Equal(t, context.appendix.Commits[2].Operation.Type, OpDeleteCollection)
}

func TestSetDocument(t *testing.T) {
	context := NewTestContext(t)

	hash, err := CreateCollection(context, "tests", "this is a test", "testCollection")

	assert.NoError(t, err)

	assert.Equal(t, 1, len(context.appendix.Commits))

	assert.Equal(t, context.appendix.Commits[0].Hash, hash)

	assert.Equal(t, "", context.appendix.Collections["testCollection"].DocumentPointers["testDocument"])

	hash, err = SetDocument(context, "tests", "this is a test", "testCollection", "testDocument", "My great test document")

	assert.NoError(t, err)

	assert.Equal(t, context.appendix.Commits[1].Hash, hash)

	assert.Equal(t, 2, len(context.appendix.Commits))

	assert.NotEqual(t, "", context.appendix.Collections["testCollection"].DocumentPointers["testDocument"])

	hash, err = SetDocument(context, "tests", "this is a test", "testCollection", "testDocument2", "My great test document")

	assert.NoError(t, err)

	assert.NotEqual(t, "", context.appendix.Collections["testCollection"].DocumentPointers["testDocument2"])

	assert.Equal(t, context.appendix.Commits[2].Hash, hash)

	assert.Equal(t, 3, len(context.appendix.Commits))

	assert.Equal(t, context.appendix.Commits[2].Operation.Type, OpSetDocument)
}

func TestDeleteDocument(t *testing.T) {
	context := NewTestContext(t)

	hash, err := CreateCollection(context, "tests", "this is a test", "testCollection")

	assert.Equal(t, context.appendix.Commits[0].Hash, hash)

	assert.NoError(t, err)

	hash, err = SetDocument(context, "tests", "this is a test", "testCollection", "testDocument", "My great test document")

	assert.Equal(t, context.appendix.Commits[1].Hash, hash)

	assert.NoError(t, err)

	hash, err = DeleteDocument(context, "tests", "this is a test", "testCollection", "testDocument")

	assert.NoError(t, err)

	assert.Equal(t, context.appendix.Commits[2].Hash, hash)

	assert.Equal(t, 3, len(context.appendix.Commits))

	assert.Equal(t, "", context.appendix.Collections["testCollection"].DocumentPointers["testDocument"])

	assert.Equal(t, context.appendix.Commits[2].Operation.Type, OpDeleteDocument)
}

func TestRollBack(t *testing.T) {
	context := NewTestContext(t)

	hash, err := CreateCollection(context, "tests", "this is a test", "collection1")

	assert.Equal(t, context.appendix.Commits[0].Hash, hash)

	assert.NoError(t, err)

	hash, err = SetDocument(context, "tests", "this is a test", "collection1", "document1", "My great test document")

	assert.Equal(t, context.appendix.Commits[1].Hash, hash)

	assert.NoError(t, err)

	hash, err = SetDocument(context, "tests", "this is a test", "collection1", "document2", "My great test document")

	assert.Equal(t, context.appendix.Commits[2].Hash, hash)

	assert.NoError(t, err)

	hash, err = CreateCollection(context, "tests", "this is a test", "collection2")

	assert.Equal(t, context.appendix.Commits[3].Hash, hash)

	assert.NoError(t, err)

	stateOneHash := context.appendix.Commits[3].Hash

	hash, err = DeleteDocument(context, "tests", "this is a test", "collection1", "document1")

	assert.Equal(t, context.appendix.Commits[4].Hash, hash)

	assert.NoError(t, err)

	hash, err = DeleteCollection(context, "tests", "this is a test", "collection2")

	assert.Equal(t, context.appendix.Commits[5].Hash, hash)

	assert.NoError(t, err)

	assert.Equal(t, 6, len(context.appendix.Commits))

	stateTwoHash := context.appendix.Commits[5].Hash

	//first rollback

	hash, err = Rollback(context, "tests", "this is a test", stateOneHash)

	assert.Equal(t, context.appendix.Commits[6].Hash, hash)

	assert.Equal(t, 7, len(context.appendix.Commits))

	assert.NoError(t, err)

	assert.NotEqual(t, "", context.appendix.Collections["collection1"].DocumentPointers["document1"])

	err, collections := ListCollections(context, context.GetLatestHash())

	assert.NoError(t, err)

	assert.Equal(t, 2, len(collections))

	//second rollback

	hash, err = Rollback(context, "tests", "this is a test", stateTwoHash)

	assert.Equal(t, context.appendix.Commits[7].Hash, hash)

	assert.Equal(t, 8, len(context.appendix.Commits))

	assert.NoError(t, err)

	assert.Equal(t, "", context.appendix.Collections["collection1"].DocumentPointers["document1"])

	err, collections = ListCollections(context, context.GetLatestHash())

	assert.NoError(t, err)

	assert.Equal(t, 1, len(collections))

	//at this point commit history looks like
	//
	// insert/create   deletes   rollbacks
	//      v            v          v
	//   o-o-o-o        o-o        o-o

	stateThreeHash := context.appendix.Commits[7].Hash

	hash, err = CreateCollection(context, "tests", "this is a test", "collection3")

	assert.Equal(t, context.appendix.Commits[8].Hash, hash)

	assert.NoError(t, err)

	hash, err = SetDocument(context, "tests", "this is a test", "collection3", "document3", "My great test document")

	assert.Equal(t, context.appendix.Commits[9].Hash, hash)

	assert.NoError(t, err)

	assert.Equal(t, 10, len(context.appendix.Commits))

	//at this point commit history looks like
	//
	//  set/create    deletes    rollbacks     set/create
	//      v            v          v              v
	//   o-o-o-o        o-o        o-o            o-o

	//third rollback

	hash, err = Rollback(context, "tests", "this is a test", stateThreeHash)

	assert.Equal(t, context.appendix.Commits[10].Hash, hash)

	assert.Equal(t, 11, len(context.appendix.Commits))

	assert.Equal(t, OpRollback, context.appendix.Commits[10].Operation.Type)

	err, collections = ListCollections(context, context.GetLatestHash())

	assert.NoError(t, err)

	assert.Equal(t, 1, len(collections))

	assert.Equal(t, "collection1", collections[0])

	//at this point commit history looks like
	//
	//  set/create    deletes    rollbacks     set/create    rollback
	//      v            v          v              v            v
	//   o-o-o-o        o-o        o-o            o-o           o

}

func TestHistoricalImmutableCommands(t *testing.T) {
	context := NewTestContext(t)

	hash, err := CreateCollection(context, "tests", "this is a test", "testCollection")

	assert.Equal(t, context.appendix.Commits[0].Hash, hash)

	assert.NoError(t, err)

	hash, err = SetDocument(context, "tests", "this is a test", "testCollection", "testDocument", "My great test document")

	assert.Equal(t, context.appendix.Commits[1].Hash, hash)

	assert.NoError(t, err)

	stateHash := context.appendix.Commits[1].Hash

	hash, err = DeleteCollection(context, "tests", "this is a test", "testCollection")

	assert.Equal(t, context.appendix.Commits[2].Hash, hash)

	assert.NoError(t, err)

	err, collections := ListCollections(context, context.GetLatestHash())

	assert.NoError(t, err)

	assert.Equal(t, 0, len(collections))

	err, collections = ListCollections(context, stateHash)

	assert.NoError(t, err)

	assert.Equal(t, 1, len(collections))
}
