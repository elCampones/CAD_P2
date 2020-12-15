package cadlabs.sql;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.List;

public class FlightsPerAirportTest extends AbstractTest<List<Row>> {

    @Override
    protected List<Row> run(Dataset<Row> flights) {
        return new FlightsPerAirport(flights).run();
    }

    @Override
    protected String expectedResult() {
        return "[[PSE,67], [INL,50], [MSY,3130], [PPG,10], [GEG,699], [SNA,3159], [BUR,1683], [GTF,149], [GRB,255], [IDA,211], "
                + "[GRR,720], [JLN,61], [PSG,55], [EUG,416], [PVD,813], [GSO,530], [MYR,106], [OAK,3319], [FAR,368], [MSN,696], "
                + "[BTM,61], [COD,55], [FSM,199], [MQT,25], [SCC,77], [DCA,5336], [CID,396], [MLU,260], [LWS,53], [GTR,79], [WRG,59], "
                + "[HLN,118], [LEX,357], [RDM,260], [ORF,884], [SCE,42], [KTN,170], [EVV,198], [CRW,205], [CWA,84], [SAV,451], "
                + "[GCK,62], [CDV,59], [TRI,159], [CMH,1955], [MOD,88], [ADK,9], [CAK,485], [TYR,212], [CHO,89], [MOB,416], [PNS,602], "
                + "[DIK,107], [CEC,80], [LIH,866], [IAH,13466], [HNL,3758], [SHV,545], [SJC,3257], [CVG,1385], [TOL,60], [LGA,7521], "
                + "[BUF,1163], [TLH,381], [CDC,52], [ACT,147], [HPN,513], [RDD,91], [AUS,3204], [MLI,288], [GCC,119], [SJU,2198], "
                + "[ATW,142], [DHN,104], [LGB,942], [AVL,206], [GJT,260], [BFL,293], [GFK,3], [RNO,1226], [SRQ,312], [EYW,399], "
                + "[SBN,243], [BJI,56], [TTN,123], [JAC,323], [RST,30], [CHS,815], [RSW,2643], [TUL,1474], [HRL,340], [AMA,608], "
                + "[ISP,372], [BOS,7589], [MLB,109], [MAF,789], [EWR,8239], [LAS,10835], [BIS,167], [FAI,322], [JAN,667], [ITO,517], "
                + "[IMT,55], [XNA,721], [DLH,103], [DEN,17293], [EWN,56], [RHI,80], [SGU,149], [ALB,541], [CPR,171], [OME,58], "
                + "[LNK,141], [GRI,57], [IAD,4263], [PSP,1139], [BOI,818], [SBA,777], [LAR,57], [HOB,43], [DRO,212], [BRO,194], "
                + "[BRD,73], [BMI,189], [RKS,147], [SEA,7663], [LAN,82], [CMI,145], [LRD,185], [VLD,79], [MCI,3401], [FLG,127], "
                + "[GRK,378], [BNA,4326], [CLT,9378], [CLL,209], [TVC,86], [BLI,76], [PSC,198], [CIC,90], [ORH,54], [PBI,2103],"
                + " [ABQ,1846], [SDF,1059], [ART,35], [ACV,252], [LAW,121], [BDL,1652], [DAL,3743], [MRY,403], [DBQ,36], [CLE,2859], "
                + "[PDX,4282], [MIA,7212], [MFR,237], [ILG,55], [TWF,107], [BWI,6775], [TPA,5219], [BKG,86], [CMX,44], [APN,52], "
                + "[OKC,1581], [ROA,136], [SMF,3311], [BRW,77], [SPI,131], [OTH,29], [ABI,240], [MBS,90], [ELM,95], [PHX,13135], "
                + "[FCA,150], [ABR,60], [STL,3818], [BET,80], [PWM,295], [ABY,82], [DFW,22771], [MHT,496], [TXK,89], [ABE,113], "
                + "[GSP,537], [LSE,51], [MMH,60], [STX,88], [FAY,172], [HDN,167], [GUC,69], [LMT,57], [LBB,501], [EKO,87], "
                + "[CRP,485], [EGE,278], [SWF,57], [FSD,511], [SUN,99], [BQK,78], [CSG,92], [SFO,13141], [MEM,1197], [SAF,141], "
                + "[ELP,1394], [BHM,971], [ATL,28420], [FLL,5585], [FNT,296], [PIH,80], [RIC,1103], [DAY,676], [PHF,164], "
                + "[OMA,1520], [SJT,148], [LCH,160], [VPS,347], [BPT,82], [MHK,133], [LIT,1040], [ICT,706], [FAT,923], [ECP,253], "
                + "[CAE,503], [ORD,17960], [AVP,52], [BTV,244], [COU,86], [MKG,43], [BIL,268], [AEX,263], [SPS,118], [ILM,165], "
                + "[SMX,101], [PIA,249], [GUM,31], [RDU,2975], [BQN,101], [MFE,322], [HIB,51], [MKE,2433], [SYR,460], [ISN,201], "
                + "[HSV,471], [LFT,435], [MTJ,147], [TUS,1413], [PIT,2172], [ROW,89], [MDW,5545], [AZO,5], [COS,703], [OAJ,121], "
                + "[JNU,253], [IND,1975], [ALO,40], [KOA,899], [EAU,43], [GPT,303], [MGM,263], [DTW,6951], [HOU,4776], [TYS,603], "
                + "[CHA,258], [ADQ,46], [YUM,260], [ONT,1644], [MDT,285], [FWA,239], [JAX,1694], [LAX,18051], [MSP,7760], [MOT,120], "
                + "[SIT,89], [BTR,622], [BGR,42], [MCO,8830], [OTZ,59], [ROC,570], [AGS,232], [SGF,491], [SAN,5875], [BZN,265], "
                + "[GGG,61], [YAK,60], [JFK,7135], [ANC,1271], [DAB,111], [PAH,46], [SUX,46], [MSO,228], [GNV,252], [OGG,1740], "
                + "[PHL,5663], [DSM,769], [FOE,43], [SLC,8579], [SAT,2607], [STT,400], [RAP,247], [ASE,548], [CLD,200], [SBP,360]]";
    }

}
