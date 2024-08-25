package file_manager

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
)

/*
FileManager used for manage the DATABASE dir, not the table files.

It also provides the communication between fileSystem Block and memory Page.
*/
type FileManager struct {
	dbDir     string
	blockSize uint64              //also the Page blockNum, fileSize / blockSize = blockNum
	isNew     bool                //if the dbDir doesn't exist, create it and set isNew as true
	openFiles map[string]*os.File //only the getFile() will add elem into it
	mu        sync.Mutex
}

func NewFileManager(dbDir string, blockSize uint64) (*FileManager, error) {
	fileManger := FileManager{
		dbDir:     dbDir,
		blockSize: blockSize,
		isNew:     false,
		openFiles: make(map[string]*os.File),
	}

	//if dbDir doesn't exist
	if _, err := os.Stat(dbDir); os.IsNotExist(err) {
		fileManger.isNew = true
		err := os.Mkdir(dbDir, os.ModeDir)
		if err != nil {
			return nil, err
		}
	} else { //if exist
		err := filepath.Walk(dbDir,
			func(path string, info os.FileInfo, err error) error {
				mode := info.Mode()
				if mode.IsRegular() {
					name := info.Name()
					//TODO: remove the tempxxxx, this file is generated when ???
					if strings.HasPrefix(name, "temp") {
						err := os.Remove(filepath.Join(path, name))
						if err != nil {
							return err
						}
					}
				}
				return nil
			})
		if err != nil {
			return nil, err
		}
	}

	return &fileManger, nil
}

func (f *FileManager) getFile(fileName string) (*os.File, error) {
	path := filepath.Join(f.dbDir, fileName)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	f.openFiles[fileName] = file

	return file, nil
}

/*
Read read the data in BlockId and store it in Page. The Block Size always fits the blockNum of Page.

- It uses a FILE_LOCK to lock the entire file
- 🤔TODO: I think that's extremely low efficiency
- The java provides some fileLock that can lock part of the file;
*/
func (f *FileManager) Read(blk *BlockId, page *Page) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	file, err := f.getFile(blk.GetFilePath())
	if err != nil {
		return 0, err
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {

		}
	}(file)

	count, err := file.ReadAt(page.contents(), int64(blk.BlkNum()*f.blockSize))
	if err != nil {
		return 0, err
	}
	return count, nil
}

/*
write data from Page to BlockId. The Block Size always fits the blockNum of Page.
*/
func (f *FileManager) Write(blk *BlockId, page *Page) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	/*FIXME: check if the file has been managed by FM before create it*/
	file, err := f.getFile(blk.GetFilePath())
	if err != nil {
		return 0, err
	}

	defer func(file *os.File) {
		err := file.Close()
		if err != nil {

		}
	}(file)

	count, err := file.WriteAt(page.contents(), int64(blk.BlkNum()*f.blockSize))

	if err != nil {
		return 0, err
	}
	return count, nil
}

/*
BlockNum returns the number of blocks in the file.
The Size() in teacher's code
*/
func (f *FileManager) BlockNum(fileName string) (uint64, error) {

	file, err := f.getFile(fileName)
	if err != nil {
		return 0, err
	}

	fileStat, err := file.Stat()
	if err != nil {
		return 0, err
	}
	return uint64(fileStat.Size()) / f.blockSize, nil
}

/*
Append uses the [blockSize]byte, empty, to expand the file by one block and returns the new blockId.
*/
func (f *FileManager) Append(fileName string) (BlockId, error) {

	newBlockNum, err := f.BlockNum(fileName)
	if err != nil {
		return BlockId{}, err
	}

	//The blockId starts from 0. Thus, if the newBlockNum is 1( indicates only 1 block in the file),
	//then this newBlock should be 1.
	newBlock := BlockId{fileName, newBlockNum}

	file, err := f.getFile(newBlock.GetFilePath())

	if err != nil {
		return BlockId{}, err
	}

	/*TODO: after appending new block, should I close the file here?*/
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			panic(err)
		}
	}(file)

	buf := make([]byte, f.blockSize)
	_, err = file.WriteAt(buf, int64(newBlock.BlkNum()*f.blockSize))
	if err != nil {
		return BlockId{}, err
	}
	return newBlock, nil
}

func (f *FileManager) IsNew() bool {
	return f.isNew
}

func (f *FileManager) BlockSize() uint64 {
	return f.blockSize
}

func (f *FileManager) IsFileEmpty(fileName string) (bool, error) {
	num, err := f.BlockNum(fileName)
	if err != nil {
		return false, nil
	}
	return num == 0, nil
}
