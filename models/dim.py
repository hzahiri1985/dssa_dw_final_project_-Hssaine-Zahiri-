class Customer(Base):
    __table_args__ = {"schema": "dssa"}
    __tablename__ = 'dim_customer'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    address = Column(VARCHAR(length=50))
    zip = Column(VARCHAR(length=10))
    city = Column(VARCHAR(length=50))
    country = Column(VARCHAR(length=50))
    payments = relationship("dssa.dim_payments", back_populates = "dssa.dim_customer")

class Payments(Base):
    __table_args__ = {"schema": "dssa"}
    __tablename__ = 'dim_payments'

    payment_id = Column(Integer, primary_key=True)
    customer_id = Column(Integer, ForeignKey('dssa.dim_customer.id'))
    amount = Column(Numeric(precision=5,scale=2))
    payment_date = Column(DateTime)
    customer = relationship("dssa.dim_customer", back_populates = "dssa.dim_payments")

class MonthlyRevenue(Base):
    __table_args__ = {"schema": "dssa"}
    __tablename__ = "monthly_revenue"

    id = Column(Integer(), primary_key=True)
    customer_id = Column(Integer, ForeignKey('dssa.dim_customer.id'))
    year = Column(Integer)
    month = Column(Integer)
    total_sales = Column(Numeric(precision=5,scale=2))
    customer = relationship("dssa.dim_customer", back_populates = "dssa.monthly_revenue")