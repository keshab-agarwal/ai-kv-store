package store

import (
	"encoding/binary"
	"io"
	"kv-store/interfaces"
	"kv-store/internal/version"
	"os"
	"path/filepath"
)

func WriteRecord(f *os.File, key interfaces.Key, value interfaces.Value, vv version.Vector) error {
	if _, err := f.Write(key[:]); err != nil {
		return err
	}
	if err := binary.Write(f, binary.LittleEndian, uint32(len(value))); err != nil {
		return err
	}
	if _, err := f.Write(value); err != nil {
		return err
	}
	for i := 0; i < 3; i++ {
		if err := binary.Write(f, binary.LittleEndian, vv[i]); err != nil {
			return err
		}
	}
	return f.Sync()
}

func (s *CausalStore) Append(path string, key interfaces.Key, value interfaces.Value, vv version.Vector) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	return WriteRecord(f, key, value, vv)
}

func (s *CausalStore) Save(path string) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	f, err := os.Create(path + ".tmp")
	if err != nil {
		return err
	}
	defer f.Close()
	for key, list := range s.entries {
		for _, v := range list {
			if _, err := f.Write(key[:]); err != nil {
				return err
			}
			if err := binary.Write(f, binary.LittleEndian, uint32(len(v.Value))); err != nil {
				return err
			}
			if _, err := f.Write(v.Value); err != nil {
				return err
			}
			for i := 0; i < 3; i++ {
				if err := binary.Write(f, binary.LittleEndian, v.VV[i]); err != nil {
					return err
				}
			}
		}
	}
	if err := f.Sync(); err != nil {
		return err
	}
	return os.Rename(path+".tmp", path)
}

func (s *CausalStore) Load(path string) error {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()
	s.mu.Lock()
	defer s.mu.Unlock()
	for {
		var key interfaces.Key
		if _, err := io.ReadFull(f, key[:]); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		var vlen uint32
		if err := binary.Read(f, binary.LittleEndian, &vlen); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if vlen > interfaces.MaxValueSize {
			return err
		}
		value := make(interfaces.Value, vlen)
		if _, err := io.ReadFull(f, value); err != nil {
			return err
		}
		var vv version.Vector
		for i := 0; i < 3; i++ {
			if err := binary.Read(f, binary.LittleEndian, &vv[i]); err != nil {
				return err
			}
		}
		s.addVersionLocked(key, Versioned{Value: value, VV: vv})
	}
	return nil
}
