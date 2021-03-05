/*
 * Cloud9: A MapReduce Library for Hadoop Licensed under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law
 * or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */

package uk.ac.man.cs.comp38211.util.pair;

public class PairOfObjects<L extends Comparable<L>, R extends Comparable<R>>
        implements Comparable<PairOfObjects<L, R>>
{

    private L left;
    private R right;

    public PairOfObjects(L left, R right)
    {
        this.left = left;
        this.right = right;
    }

    public PairOfObjects()
    {
    }

    public L getLeftElement()
    {
        return left;
    }

    public R getRightElement()
    {
        return right;
    }

    public void set(L left, R right)
    {
        this.left = left;
        this.right = right;
    }

    public void setLeftElement(L left)
    {
        this.left = left;
    }

    public void setRightElement(R right)
    {
        this.right = right;
    }

    /**
     * Generates human-readable String representation of this pair.
     */
    public String toString()
    {
        return "(" + left + ", " + right + ")";
    }

    /**
     * Creates a shallow clone of this object; the left and right elements are
     * not cloned.
     */
    public PairOfObjects<L, R> clone()
    {
        return new PairOfObjects<L, R>(left, right);
    }

    @Override
    public int compareTo(PairOfObjects<L, R> that)
    {
        if (this.left.equals(that.left))
        {
            return this.right.compareTo(that.right);
        }
        return this.left.compareTo(that.left);
    }
}