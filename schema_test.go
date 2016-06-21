package db_test

import (
	"fmt"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	// sql drivers
	_ "github.com/mattn/go-sqlite3"

	"github.com/jhunt/db"
)

func Database(sqls ...string) (*db.DB, error) {
	var d *db.DB
	d = &db.DB{
		Driver: "sqlite3",
		DSN:    ":memory:",
	}

	if err := d.Connect(); err != nil {
		return nil, err
	}

	schema := db.NewSchema()
	schema.Version(1, func(d *db.DB) error {
		err := d.Exec(
			`CREATE TABLE foo (
  id INTEGER(11) PRIMARY KEY,
  value TEXT
)`)
		if err != nil {
			return err
		}
		return nil
	})
	if err := schema.Migrate(d, db.Latest); err != nil {
		d.Disconnect()
		return nil, err
	}

	for _, s := range sqls {
		err := d.Exec(s)
		if err != nil {
			d.Disconnect()
			return nil, err
		}
	}

	return d, nil
}

var _ = Describe("Database Schema", func() {
	Describe("Initializing the schema", func() {
		Context("With a new database", func() {
			var d *db.DB
			var s *db.Schema

			BeforeEach(func() {
				d = &db.DB{
					Driver: "sqlite3",
					DSN:    ":memory:",
				}

				Ω(d.Connect()).Should(Succeed())
				Ω(d.Connected()).Should(BeTrue())

				s = db.NewSchema()
				s.Version(1, func(d *db.DB) error {
					err := d.Exec(`CREATE TABLE foo (id INTEGER(11) PRIMARY KEY, value TEXT)`)
					if err != nil {
						return err
					}
					return nil
				})
			})

			It("should not create tables until schema.Migrate() is called", func() {
				Ω(d.Exec("SELECT * FROM schema_info")).
					Should(HaveOccurred())
			})

			It("should create tables during schema.Migrate()", func() {
				Ω(s.Migrate(d, db.Latest)).Should(Succeed())
				Ω(d.Exec("SELECT * FROM schema_info")).
					Should(Succeed())
			})

			It("should set the version number in schema_info", func() {
				Ω(s.Migrate(d, db.Latest)).Should(Succeed())

				r, err := d.Query(`SELECT version FROM schema_info`)
				Ω(err).ShouldNot(HaveOccurred())
				Ω(r).ShouldNot(BeNil())
				Ω(r.Next()).Should(BeTrue())

				var v int
				Ω(r.Scan(&v)).Should(Succeed())
				Ω(v).Should(Equal(1))
			})

			It("creates the correct tables", func() {
				Ω(s.Migrate(d, db.Latest)).Should(Succeed())

				sql := fmt.Sprintf("SELECT * FROM foo")
				Ω(d.Exec(sql)).Should(Succeed())
			})
		})
	})

	Describe("Schema Version Interrogation", func() {
		It("should return an error for a bad database connection", func() {
			d := &db.DB{
				Driver: "postgres",
				DSN:    "host=127.86.86.86, port=8686",
			}
			s := db.NewSchema()

			d.Connect()
			_, err := s.Current(d)
			Ω(err).Should(HaveOccurred())
		})
	})
})
