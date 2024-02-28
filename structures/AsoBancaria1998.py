from dataclasses import dataclass, field


@dataclass
class AsoBancaria1998:
    mainCompanyCode: str
    firstExpiryDate: str
    secondExpiryDate: str
    billingDate: str
    cycle: str
    filler: str = field(default='' * 42)  # 42 is the default filler for Aso-1998 format
    registerType: str = field(default='01', )

    def __str__(self):
        return f'{self.registerType}, ' \
               f'{self.mainCompanyCode}, ' \
               f'{self.firstExpiryDate}, ' \
               f'{self.secondExpiryDate}, ' \
               f'{self.billingDate}, ' \
               f'{self.cycle}, ' \
               f'{self.filler} '


asoBancaria1998_1 = AsoBancaria1998('no', '123', '01012023', '01012023', '01012023', '02', '000000000000')
asoBancaria1998_2 = AsoBancaria1998('no', '123', '01012023', '01012024', '01012023', '02', '000000000000')
print(asoBancaria1998_1)
print(asoBancaria1998_1 == asoBancaria1998_2)
