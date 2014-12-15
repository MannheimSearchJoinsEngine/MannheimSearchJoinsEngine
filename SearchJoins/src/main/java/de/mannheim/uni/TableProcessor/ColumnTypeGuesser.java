package de.mannheim.uni.TableProcessor;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import de.mannheim.uni.model.TableColumn.ColumnDataType;
import de.mannheim.uni.parsers.BooleanParser;
import de.mannheim.uni.parsers.DateUtil;
import de.mannheim.uni.parsers.GeoCoordinateParser;
import de.mannheim.uni.parsers.NumericParser;
import de.mannheim.uni.parsers.URLParser;
import de.mannheim.uni.parsers.UnitParser;
import de.mannheim.uni.units.SubUnit;

/**
 * @author petar
 * 
 */
public class ColumnTypeGuesser {
	private UnitParser unitParser;

	public ColumnTypeGuesser() {
		unitParser = new UnitParser();
	}

	/**
	 * use for rough type guesssing
	 * 
	 * @param columnValue
	 *            is the value of the column
	 * @param columnHeader
	 *            is the header of the column, often contains units
	 *            abbreviations
	 * @param useUnit
	 *            the typeGuesser will try to find units
	 * @param unit
	 *            the returning unit (if found)
	 * @return
	 */
	public ColumnDataType guessTypeForValue(String columnValue,
			String columnHeader, boolean useUnit, SubUnit unit) {

		if (columnValue.matches("^\\{.+\\|.+\\}$"))
			return ColumnDataType.list;
		// check the length
		boolean validLenght = true;
		if (columnValue.length() > 50) {
			useUnit = false;
			validLenght = false;
		}
		if (useUnit) {

			SubUnit unitS = null;
			if (columnHeader != null) {

				unitS = unitParser.parseUnit(columnValue + " "
						+ extractUnitAbbrFromHeader(columnHeader));
			}
			if (unitS == null) {
				unitS = unitParser.parseUnit(columnValue);
			}
			if (unitS != null) {
				unit.setAbbrevations(unitS.getAbbrevations());
				unit.setBaseUnit(unitS.getBaseUnit());
				unit.setConvertible(unitS.isConvertible());
				unit.setName(unitS.getName());
				unit.setNewValue(unitS.getNewValue());
				unit.setRateToConvert(unitS.getRateToConvert());
				return ColumnDataType.unit;
			}
		}
		if (validLenght)
			try {
				Date date = DateUtil.parse(columnValue);
				if (date != null)
					return ColumnDataType.date;
			} catch (Exception e) {

			}

		if (validLenght && Boolean.parseBoolean(columnValue))
			return ColumnDataType.bool;

		if (URLParser.parseURL(columnValue))
			return ColumnDataType.link;

		if (validLenght && GeoCoordinateParser.parseGeoCoordinate(columnValue))
			return ColumnDataType.coordinate;

		if (validLenght && NumericParser.parseNumeric(columnValue)) {
			return ColumnDataType.numeric;
		}

		return ColumnDataType.string;

	}

	/**
	 * Returns the value from brackets
	 * 
	 * @param header
	 * @return
	 */
	private static String extractUnitAbbrFromHeader(String header) {
		try {
			if (header.matches(".*\\(.*\\).*"))
				return header.substring(header.indexOf("(") + 1,
						header.indexOf(")")).replaceAll("\\.", "");
		} catch (Exception e) {
			
		}

		return header;
	}

	public static void main(String[] args) {
		// example to extract a unit
		ColumnTypeGuesser g = new ColumnTypeGuesser();

		// list
		System.out.println(g.guessTypeForValue("{value1|value2}", null, false,
				null));

		// date
		System.out
				.println(g.guessTypeForValue("10/31/2012", null, false, null));

		// coordinate
		System.out.println(g.guessTypeForValue("41.1775 20.6788", null, false,
				null));

		// guess units
		SubUnit subUnit = new SubUnit();
		g.guessTypeForValue("3000", "area           (sq. km.)(", true, subUnit);
		//
		// g.guessTypeForValue(
		// "8 9 11 13 14 15 16 21 23 24 26 27 28 29 30 31 32 33 35 37 39 40 43 48 49 51 52 54 56 57 59 65 66 67 68 69 70 71 72 83 84 85 94 96 97 98 102 104 106 125 126 130 131 136 137 138 141 143 146 147 148 149 156 157 158 159 160 162 163 164 169 171 172 173 174 18 0 181 184 185 186 187 199 200 201 202 205 206 207 208 209 210 211 212 213 214 215 216 217 218 219 220 221 222 223 224 225 227 228 229 230 231 232 234 235 236 237 241 243 247 248 249 250 251 252 253 254 255 256 257 258 259 260 261 262 263 264 268 269 273 2 74 276 278 280 281 282 283 284 285 286 287 288 289 290 291 292 301 302 303 304 305 306 307 310 311 312 313 314 315 316 317 318 319 320 321 322 323 324 325 326 327 328 329 330 331 332 333 344 345 346 347 348 349 350 354 355 356 357 358 359 360 365 366 367 368 369 370 371 372 373 374 375 376 378 380 384 385 386 387 389 390 402 438 439 445 446 447 448 449 450 451 452 455 456 458 459 460 461 462 463 464 465 466 467 469 470 471 472 473 474 475 476 477 478 479 480 481 482 483 484 485 486 487 488 489 490 491 492 493 494 496 497 498 499 500 502 503 504 513 514 515 516 518 519 520 521 522 523 524 525 526 527 530 531 532 533 534 535 536 537 538 540 554 555 556 557 558 559 560 561 562 563 564 566 567 568 569 570 572 574 575 576 577 578 579 580 581 582 583 584 585 58 6 600 601 602 603 604 605 606 607 608 611 612 613 614 616 619 620 621 622 626 627 630 631 632 633 634 635 636 637 638 639 640 641 642 643 644 645 648 655 656 657 658 659 660 661 663 665 666 667 669 670 671 672 673 674 675 676 677 678 679 680 681 682 683 6 84 685",
		// "sas", true, subUnit);

		String baseUnit = subUnit.getBaseUnit().getName();
		String normalizedValue = subUnit.getNewValue();
		System.out.println("3000 area (sq km) was converted to: "
				+ normalizedValue + " "
				+ subUnit.getBaseUnit().getMainUnit().getName() + " "
				+ baseUnit);

	}

}
